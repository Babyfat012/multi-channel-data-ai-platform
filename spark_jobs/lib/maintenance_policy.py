"""Pure-Python Iceberg policy: plan, thresholds, retention and retry with receipts."""

from __future__ import annotations

import json
import math
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from spark_jobs.lib.return_label import utc

DEFAULTS = {
    "min_input_files": 5,
    "small_file_ratio": 0.5,
    "target_file_size_bytes": 134217728,
    "zorder_columns": [],
    "zorder_min_new_files": 20,
    "max_snapshot_age_hours": 168,
    "retain_last": 3,
    "generation_tag_prefix": "gen-",
    "orphans_dry_run": True,
    "orphan_age_hours": 72,
    "max_attempts": 3,
    "backoff_seconds": 1,
    "max_backoff_seconds": 8,
    "probe_predicates": [],
}


def validated_config(config):
    """Normalize config, rejecting unsafe retention/retry and invalid columns."""
    result = {**DEFAULTS, **config}
    for key in (
        "min_input_files",
        "target_file_size_bytes",
        "zorder_min_new_files",
        "retain_last",
        "max_attempts",
    ):
        if type(result[key]) is not int or result[key] < 1:
            raise ValueError(f"{key} must be a positive integer")
    for key in (
        "small_file_ratio",
        "max_snapshot_age_hours",
        "orphan_age_hours",
        "backoff_seconds",
        "max_backoff_seconds",
    ):
        if (
            type(result[key]) not in (int, float)
            or not math.isfinite(result[key])
            or result[key] < 0
        ):
            raise ValueError(f"{key} is invalid")
    if not 0 < result["small_file_ratio"] <= 1 or result["orphan_age_hours"] < 72:
        raise ValueError("ratio must be in (0,1]; orphan_age_hours must be >=72")
    if type(result["orphans_dry_run"]) is not bool:
        raise ValueError("orphans_dry_run must be a boolean")
    if not isinstance(result["generation_tag_prefix"], str) or not result["generation_tag_prefix"]:
        raise ValueError("generation_tag_prefix must be non-empty")
    if not isinstance(result["zorder_columns"], list) or any(
        not isinstance(column, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column)
        for column in result["zorder_columns"]
    ):
        raise ValueError("zorder_columns must be a list of single column names")
    if not isinstance(result["probe_predicates"], list):
        raise ValueError("probe_predicates must be a list")
    for probe in result["probe_predicates"]:
        if not isinstance(probe, dict) or not probe.get("name") or not probe.get("ranges"):
            raise ValueError("probe needs name and ranges (AND of closed ranges)")
        for column, bounds in probe["ranges"].items():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) or len(bounds) != 2:
                raise ValueError("Invalid probe range")
            if bounds[0] is not None and bounds[1] is not None and bounds[0] > bounds[1]:
                raise ValueError("Probe range is reversed")
    return result


def load_config(path):
    """Read the per-table JSON config; never guess real tables when config is missing."""
    data = json.loads(Path(path).read_text())
    if not isinstance(data.get("tables"), dict) or not data["tables"]:
        raise ValueError("Config needs non-empty tables")
    return {table: validated_config(config) for table, config in data["tables"].items()}


def plan_maintenance(metadata, config, now=None):
    """Return actions with reasons; the orphan dry-run is always present, no_op only considers table changes.

    The brief requires both that orphans are always planned and that a second run has empty actions.
    Keep the orphan safety rule: no_op=true means no mutation,
    and actions still contain exactly the orphan dry-run inventory.
    """
    config = validated_config(config)
    now = utc(now or datetime.now(timezone.utc))
    actions = []
    grouped = defaultdict(list)
    for file in metadata.get("files", []):
        if file.get("content", 0) == 0:
            grouped[json.dumps(file.get("partition", {}), sort_keys=True, default=str)].append(file)
    partitions = [
        json.loads(key)
        for key, files in grouped.items()
        if len(files) >= config["min_input_files"]
        and sum(file["file_size_in_bytes"] for file in files) / len(files)
        < config["small_file_ratio"] * config["target_file_size_bytes"]
    ]
    if partitions:
        actions.append(
            {
                "kind": "compact",
                "partitions": partitions,
                "target_file_size_bytes": config["target_file_size_bytes"],
                "min_input_files": config["min_input_files"],
                "reason": "Partition has enough files and average size below threshold",
            }
        )
    snapshots = sorted(metadata.get("snapshots", []), key=lambda s: utc(s["committed_at"]))
    marker = metadata.get("properties", {}).get("platform.maintenance.last_zorder_snapshot_id")
    by_id = {str(s["snapshot_id"]): s for s in snapshots}
    # Follow parents so files on branches outside the current state are not counted.
    cursor = str(
        metadata.get("current_snapshot_id", snapshots[-1]["snapshot_id"] if snapshots else "")
    )
    added, seen = 0, set()
    while cursor in by_id and cursor != str(marker) and cursor not in seen:
        seen.add(cursor)
        snapshot = by_id[cursor]
        added += int(snapshot.get("summary", {}).get("added-data-files", 0))
        cursor = str(snapshot.get("parent_id"))
    marker_missing = marker is None or cursor != str(marker)
    if config["zorder_columns"] and (marker_missing or added >= config["zorder_min_new_files"]):
        actions.append(
            {
                "kind": "zorder",
                "columns": config["zorder_columns"],
                "target_file_size_bytes": config["target_file_size_bytes"],
                "new_data_files": added,
                "reason": "No z-order marker yet/marker no longer in ancestry"
                if marker_missing
                else "Enough new data files since the z-order marker",
            }
        )
    protected = sorted({int(ref["snapshot_id"]) for ref in metadata.get("refs", [])})
    recent = {s["snapshot_id"] for s in snapshots[-config["retain_last"] :]}
    cutoff = now - timedelta(hours=config["max_snapshot_age_hours"])
    expired = [
        s["snapshot_id"]
        for s in snapshots
        if utc(s["committed_at"]) < cutoff
        and s["snapshot_id"] not in recent
        and int(s["snapshot_id"]) not in protected
    ]
    if expired:
        actions.append(
            {
                "kind": "expire_snapshots",
                "snapshot_ids": expired,
                "older_than": cutoff.isoformat(),
                "retain_last": config["retain_last"],
                "reason": "Snapshot is past age, outside retain_last and not protected by a ref",
            }
        )
    actions.append(
        {
            "kind": "remove_orphans",
            "dry_run": config["orphans_dry_run"],
            "older_than": (now - timedelta(hours=config["orphan_age_hours"])).isoformat(),
            "reason": "Orphan inventory does not delete by default; cutoff at least 72 hours",
        }
    )
    no_op = all(a["kind"] == "remove_orphans" and a["dry_run"] for a in actions)
    return {
        "actions": actions,
        "protected_snapshot_ids": protected,
        "generation_tags": [
            r["name"]
            for r in metadata.get("refs", [])
            if r.get("type", "").lower() == "tag"
            and r["name"].startswith(config["generation_tag_prefix"])
        ],
        "no_op": no_op,
        "reason": "no-op: no table-changing action; orphan inventory only"
        if no_op
        else "A maintenance threshold was exceeded",
    }


def is_transient(error):
    """Retry only commit conflicts and ValidationExceptions that indicate concurrent writes."""
    message = f"{type(error).__name__}: {error}"
    return "CommitFailedException" in message or (
        "ValidationException" in message
        and any(
            token in message.lower()
            for token in (
                "concurrent",
                "conflicting",
                "found new",
                "cannot commit",
                "changed",
                "already been committed",
            )
        )
    )


def execute_actions(actions, execute, config, attempts, sleep=time.sleep):
    """Execute sequentially; record every attempt including errors, then re-raise and stop immediately."""
    config = validated_config(config)
    for action in actions:
        for number in range(1, config["max_attempts"] + 1):
            attempt = {"kind": action["kind"], "attempt": number}
            attempts.append(attempt)
            try:
                result = execute(action)
                attempt.update(status="success", result=result)
                break
            except Exception as error:
                transient = is_transient(error)
                attempt.update(
                    status="failed", transient=transient, error=f"{type(error).__name__}: {error}"
                )
                if not transient or number == config["max_attempts"]:
                    raise
                delay = min(
                    config["max_backoff_seconds"], config["backoff_seconds"] * 2 ** (number - 1)
                )
                attempt["backoff_seconds"] = delay
                sleep(delay)


def count_probe_files(files, ranges):
    """Count files that intersect all closed ranges; files without metrics must always be read.

    This is a min/max estimate, not the number of Spark tasks or files actually read.
    ISO date/time config values are converted to the metrics type before comparison.
    """
    count = 0
    for file in files:
        overlaps = True
        metrics = file.get("readable_metrics") or {}
        for column, (low, high) in ranges.items():
            metric = metrics.get(column) or {}
            lower, upper = metric.get("lower_bound"), metric.get("upper_bound")
            example = lower if lower is not None else upper
            if isinstance(example, datetime):
                lower = utc(lower) if lower is not None else None
                upper = utc(upper) if upper is not None else None
                low = utc(low) if low is not None else None
                high = utc(high) if high is not None else None
            elif isinstance(example, date):
                low = date.fromisoformat(low) if isinstance(low, str) else low
                high = date.fromisoformat(high) if isinstance(high, str) else high
            if (lower is not None and high is not None and lower > high) or (
                upper is not None and low is not None and upper < low
            ):
                overlaps = False
                break
        count += int(overlaps)
    return count


def ensure_refs_survive_expiration(metadata, now=None):
    """Refuse expiry if the TTL would make Iceberg drop a ref the policy protects.

    Iceberg may drop an over-age ref itself in expire_snapshots. Do not rely only
    on a post-check: check retention explicitly before calling the delete procedure.
    """
    now = utc(now or datetime.now(timezone.utc))
    snapshots = {int(s["snapshot_id"]): s for s in metadata.get("snapshots", [])}
    default_age = int(
        metadata.get("properties", {}).get("history.expire.max-ref-age-ms", 9223372036854775807)
    )
    for ref in metadata.get("refs", []):
        if ref["name"] == "main":
            continue
        age_limit = ref.get("max_reference_age_in_ms")
        age_limit = default_age if age_limit is None else int(age_limit)
        snapshot = snapshots.get(int(ref["snapshot_id"]))
        if snapshot is None:
            raise ValueError(f"Ref {ref['name']} points to an unreadable snapshot")
        age_ms = (now - utc(snapshot["committed_at"])).total_seconds() * 1000
        if age_ms > age_limit:
            raise ValueError(
                f"Ref {ref['name']} is past its TTL; stopping before expiry to protect the generation"
            )
