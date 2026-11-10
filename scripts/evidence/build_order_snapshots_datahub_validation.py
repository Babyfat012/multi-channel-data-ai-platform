"""Build DataHub validation evidence from successful Airflow task logs.

Mount the PLATFORM Airflow volume read-only at ``--logs-root``. Every check is
parsed from the successful attempt of a specific documented DAG run.

The two validators log in two formats. The old format (runs 2026-10-13/14) is the
historical evidence of CDC v1 and the old feature validator. The new format also prints
`extra`/`payload mismatch` (CDC v2) and a key/time dict (feature). The parser reads both
and attaches the log format to the metrics; to use a new run, pass a separate ``--run``
and ``--output`` so the historical manifest is not overwritten.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
from datetime import datetime, timezone
from pathlib import Path

RUNS = {
    "dp1_bronze": "manual__2026-10-13T15:30:09+00:00",
    "dp1_cdc": "manual__2026-10-13T22:34:22+00:00",
    "dp2_silver": "manual__2026-10-14T03:53:52+00:00",
    "dp3_features": "manual__2026-10-14T04:11:15+00:00",
    # No default run (the old Walmart-only version ran 2026-10-14): pass --run dp3_return_labels=...
    "dp3_return_labels": "",
    "dp4_warehouse": "",
}
CHANNELS = ("amazon", "walmart", "tiktok_shop", "shopify", "shopee")
LEGACY_CDC_TABLE = "lakehouse.bronze.raw_walmart_cdc"
# Metrics whose names start with these prefixes must be 0 for the check to PASS.
FAILURE_PREFIXES = (
    "invalid",
    "duplicate",
    "missing",
    "extra",
    "bad_",
    "multiple_current",
    "payload_mismatch",
    "wrong_",
    "source_invalid",
    "source_duplicate",
)


def number(pattern: str, log: str) -> int:
    """Extract a comma-separated integer from a task log."""
    match = re.search(pattern, log)
    if match is None:
        raise ValueError(f"Missing metric: {pattern}")
    return int(match.group(1).replace(",", ""))


def success_log(root: Path, dag: str, task: str, run_id: str) -> tuple[str, str, str]:
    """Latest successful attempt: relative path and completion time."""
    directory = root / f"dag_id={dag}" / f"run_id={run_id}" / f"task_id={task}"
    attempts = sorted(
        directory.glob("attempt=*.log"),
        key=lambda path: int(path.stem.partition("=")[2]),
        reverse=True,
    )
    for path in attempts:
        log = path.read_text(errors="replace")
        match = re.search(r"Marking task as SUCCESS\..*end_date=(\d{8}T\d{6})", log)
        if match:
            completed = datetime.strptime(match.group(1), "%Y%m%dT%H%M%S")
            return (
                log,
                path.relative_to(root).as_posix(),
                completed.replace(tzinfo=timezone.utc).isoformat(),
            )
    raise ValueError(f"No successful attempt for {dag}.{task} in {directory}")


def cdc_metrics(log: str) -> tuple[dict[str, int], str, str]:
    """Parse the CDC validator; the v2 format adds extra offsets and payload mismatches."""
    metrics = {
        "source_rows": number(r"Kafka=([\d,]+)", log),
        "dataset_rows": number(r"Bronze=([\d,]+)", log),
        "unique_offsets": number(r"unique offsets=([\d,]+)", log),
        "missing_rows": number(r"missing=([\d,]+)", log),
        "invalid_rows": number(r"invalid=([\d,]+)", log),
    }
    if "extra=" not in log:
        return metrics, "Kafka = Bronze = unique offset count; no missing and no invalid", "cdc-v1"
    metrics["extra_rows"] = number(r"extra=([\d,]+)", log)
    metrics["payload_mismatch_rows"] = number(r"payload mismatch=([\d,]+)", log)
    rule = (
        "Within the run's frozen offset range and matching source epoch: Kafka = Bronze "
        "= unique offset count; no missing, no extra, payload matches, no invalid"
    )
    return metrics, rule, "cdc-v2"


def feature_metrics(log: str) -> tuple[dict[str, int], str, str]:
    """Parse the feature validator; the new format prints a dict comparing keys and event time with Gold."""
    match = re.search(r"(\{'fact_rows': [^\n]+?\})", log)
    if match is None:
        metrics = {
            "source_rows": number(r"Gold fact=([\d,]+)", log),
            "dataset_rows": number(r"feature=([\d,]+)", log),
            "invalid_rows": number(r"feature invalid=([\d,]+)", log),
            "duplicate_order_line_key": number(r"duplicate_order_line_key=([\d,]+)", log),
        }
        return (
            metrics,
            "Feature equals Gold; no invalid, no duplicate keys, no label leakage",
            "feature-v1",
        )
    raw = ast.literal_eval(match.group(1))
    metrics = {
        "source_rows": raw["fact_rows"],
        "dataset_rows": raw["feature_rows"],
        "invalid_rows": raw["invalid"],
        "duplicate_order_line_key": raw["duplicate_keys"],
        "source_invalid_rows": raw["source_invalid"],
        "source_duplicate_keys": raw["source_duplicates"],
        "missing_rows": raw["missing"],
        "extra_rows": raw["extra"],
        "wrong_event_time_rows": raw["wrong_event_time"],
    }
    rule = (
        "Feature and Gold share the same Order Line key set (no missing, no extra), no duplicate "
        "keys, event_timestamp = Gold created_at, no label leakage. Not full PIT yet"
    )
    return metrics, rule, "feature-v2"


def metrics_for(dag: str, task: str, log: str) -> tuple[dict[str, int], str, str]:
    """Read the counts, check rule and log format of one Spark validate run."""
    log_format = "v1"
    if dag == "dp1_bronze":
        if task in {"validate_amazon", "validate_walmart"}:
            metrics = {
                "source_rows": number(r"source=([\d,]+)", log),
                "dataset_rows": number(r"Bronze=([\d,]+)", log),
                "invalid_rows": number(r"invalid=([\d,]+) table=", log),
            }
        else:
            metrics = {
                "source_rows": number(r"source rows\s*=\s*([\d,]+)", log),
                "dataset_rows": number(r"Bronze rows\s*=\s*([\d,]+)", log),
                "invalid_rows": number(r"invalid rows\s*=\s*([\d,]+)", log),
            }
        rule = "Source has rows; source row count = Bronze; data and provenance valid"
        if metrics["source_rows"] <= 0 or metrics["source_rows"] != metrics["dataset_rows"]:
            raise ValueError(f"Bronze row mismatch: {task}")
    elif dag == "dp1_cdc":
        metrics, rule, log_format = cdc_metrics(log)
        if not metrics["source_rows"] == metrics["dataset_rows"] == metrics["unique_offsets"]:
            raise ValueError("CDC row mismatch")
    elif dag == "dp2_silver" and task.endswith("_silver"):
        source_pattern = r"source Orders(?: unique)?\s*=\s*([\d,]+)"
        target_pattern = r"Order Line Silver\s*=\s*([\d,]+)"
        duplicate_match = re.search(r"duplicate Order Line keys\s*=\s*(True|False)", log)
        if duplicate_match is None:
            raise ValueError(f"Missing duplicate-key result: {task}")
        metrics = {
            "source_orders": number(source_pattern, log),
            "dataset_rows": number(target_pattern, log),
            "invalid_rows": number(r"invalid rows\s*=\s*([\d,]+)", log),
            "duplicate_keys": int(duplicate_match.group(1) == "True"),
        }
        rule = "Order Line >= source Orders; required columns, run_id and keys valid"
        if metrics["source_orders"] <= 0 or metrics["dataset_rows"] < metrics["source_orders"]:
            raise ValueError(f"Silver row mismatch: {task}")
    elif task == "validate_gold_order_lines":
        match = re.search(r"Silver rows: (\{[^\n]+?\}); Gold rows: (\{[^\n]+?\})", log)
        if match is None:
            raise ValueError("Missing Gold channel counts")
        silver, gold = ast.literal_eval(match.group(1)), ast.literal_eval(match.group(2))
        if silver != gold:
            raise ValueError("Gold channel mismatch")
        metrics = {
            "source_rows": sum(silver.values()),
            "dataset_rows": sum(gold.values()),
            "invalid_rows": number(r"Gold invalid=([\d,]+)", log),
            "duplicate_order_line_key": number(r"duplicate_order_line_key=([\d,]+)", log),
        }
        rule = "Gold matches Silver per Channel; no invalid and no duplicate keys"
    elif task == "validate_amazon_order_dim":
        current = number(r"is_current=True, count=([\d,]+)", log)
        historic = number(r"is_current=False, count=([\d,]+)", log)
        metrics = {"dataset_rows": current + historic}
        for key in ("invalid", "duplicate", "multiple_current", "bad_fact", "missing_version"):
            metrics[f"{key}_rows"] = number(rf"{key}=([\d,]+)", log)
        rule = "One current version per Order; fact keys point to the correct version; no invalid"
    elif task == "validate_return_features":
        metrics, rule, log_format = feature_metrics(log)
        if metrics["source_rows"] != metrics["dataset_rows"]:
            raise ValueError("Feature row mismatch")
    elif task == "build_and_validate_return_labels":
        metrics = {
            "dataset_rows": number(r"Label bridge: rows=([\d,]+)", log),
            "duplicate_keys": number(r"Label bridge: rows=[\d,]+, duplicate=([\d,]+)", log),
            "invalid_rows": number(
                r"Label bridge: rows=[\d,]+, duplicate=[\d,]+, invalid=([\d,]+)", log
            ),
        }
        rule = (
            "Five-Channel label bridge has rows in every Channel; no invalid and no duplicate keys"
        )
    elif task == "export_and_index_warehouse":
        match = re.search(r"gold=(\{[^}]*\}) postgres=(\{[^}]*\})", log)
        if match is None:
            raise ValueError("Missing metric: gold/postgres warehouse")
        gold, loaded = (ast.literal_eval(part) for part in match.groups())
        metrics = {
            "dataset_rows": number(r"index seller_sku: \S+; rows=([\d,]+)", log),
            "source_rows": sum(gold.values()),
            "bad_channel_counts": sum(gold.get(c, 0) != loaded.get(c, 0) for c in CHANNELS),
            "missing_channels": sum(gold.get(c, 0) <= 0 for c in CHANNELS),
        }
        rule = (
            "30-day warehouse matches Gold per Channel (all 5 Channels) before creating the index"
        )
    else:
        raise ValueError(f"Unknown validation task: {dag}.{task}")
    if metrics.get("dataset_rows", 0) <= 0 or any(
        value != 0 for key, value in metrics.items() if key.startswith(FAILURE_PREFIXES)
    ):
        raise ValueError(f"Validation failed: {dag}.{task}: {metrics}")
    return metrics, rule, log_format


def check_cdc_table(log_format: str, table: str) -> None:
    """Block attaching a CDC v2 log to the v1 table (or vice versa) in DataHub."""
    if (log_format == "cdc-v2") == (table == LEGACY_CDC_TABLE):
        raise ValueError(f"Log {log_format} does not match CDC table {table}")


def parse_runs(overrides: list[str]) -> dict[str, str]:
    """Merge the default runs with the given ``dag=run_id`` overrides."""
    runs = dict(RUNS)
    for item in overrides:
        dag, sep, run_id = item.partition("=")
        if not sep or dag not in RUNS or not run_id:
            raise ValueError(f"--run must be dag=run_id with dag in {sorted(RUNS)}: {item!r}")
        runs[dag] = run_id
    return runs


def main() -> None:
    """Write the manifest only when every selected check is readable and PASSes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs-root", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, default=Path("docs/evidence/datahub-validation.json")
    )
    parser.add_argument(
        "--run",
        action="append",
        default=[],
        metavar="DAG=RUN_ID",
        help="Override the default run of one DAG; repeat for several DAGs",
    )
    parser.add_argument(
        "--cdc-table",
        default=LEGACY_CDC_TABLE,
        help="CDC table the dp1_cdc run wrote (Airflow Variable platform_dp1_cdc_table)",
    )
    parser.add_argument(
        "--dags",
        nargs="+",
        choices=sorted(RUNS),
        default=sorted(RUNS),
        help="Only read DAGs that actually ran in this environment (default: all of them)",
    )
    args = parser.parse_args()
    runs = parse_runs(args.run)
    tasks = (
        [
            ("dp1_bronze", f"validate_{channel}", f"lakehouse.bronze.raw_{channel}_orders_dp1")
            for channel in CHANNELS
        ]
        + [("dp1_cdc", "validate_walmart_cdc", args.cdc_table)]
        + [
            (
                "dp2_silver",
                f"validate_{channel}_silver",
                f"lakehouse.silver.stg_{channel}_order_lines_dp2",
            )
            for channel in CHANNELS
        ]
        + [("dp2_silver", "validate_amazon_order_dim", "lakehouse.gold.dim_amazon_order_dp2")]
        + [("dp2_silver", "validate_gold_order_lines", "lakehouse.gold.fact_order_lines_dp2")]
        + [
            (
                "dp3_features",
                "validate_return_features",
                "lakehouse.gold.feat_order_line_return_dp3",
            )
        ]
        + [
            (
                "dp3_return_labels",
                "build_and_validate_return_labels",
                "lakehouse.gold.fact_return_labels_dp3",
            )
        ]
        + [("dp4_warehouse", "export_and_index_warehouse", "warehouse.fact_order_lines_30d_dp2")]
    )
    checks = []
    for dag, task, dataset in (t for t in tasks if t[0] in args.dags):
        log, source_log, completed_at = success_log(args.logs_root, dag, task, runs[dag])
        metrics, rule, log_format = metrics_for(dag, task, log)
        if dag == "dp1_cdc":
            check_cdc_table(log_format, dataset)
        checks.append(
            {
                "dag_id": dag,
                "task_id": task,
                "dataset": dataset,
                "run_id": runs[dag],
                "state": "success",
                "completed_at": completed_at,
                "source_log": source_log,
                "rule": rule,
                "log_format": log_format,
                "metrics": metrics,
            }
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"checks": checks}, ensure_ascii=False, indent=2) + "\n")
    print(f"Wrote {len(checks)} successful checks to {args.output}")


if __name__ == "__main__":
    main()
