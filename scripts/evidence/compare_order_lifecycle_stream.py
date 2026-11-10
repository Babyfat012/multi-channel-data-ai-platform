"""Reconcile Flink JSON files against the oracle, checking both identity and per-epoch measures."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def read_jsonl(root):
    """Read committed part files, skipping hidden files and filesystem sink metadata."""
    rows = []
    root = Path(root)
    if not root.exists():
        return rows
    for path in sorted(root.rglob("*")):
        if path.is_file() and not any(
            p.startswith((".", "_")) for p in path.relative_to(root).parts
        ):
            for line in path.read_text().splitlines():
                if line.strip():
                    row = json.loads(line)
                    for part in path.relative_to(root).parts[:-1]:
                        if part.startswith("route="):
                            row.setdefault("route", part.split("=", 1)[1])
                    rows.append(row)
    return rows


def bag(rows, fields):
    """Count a multiset to detect missing, extra and duplicate rows after a restart."""
    return Counter(tuple(r[k] for k in fields) for r in rows)


def compare(root, oracle, late_oracle=None):
    """Return JSON per check; main+late must be exact and the single-partition late proof must equal the oracle per topic."""
    root, epoch = Path(root), str(oracle["epoch"])
    baseline = read_jsonl(root / "baseline" / epoch / "counts")
    routed = read_jsonl(root / "late" / epoch / "routes")
    kept = read_jsonl(root / "dedup" / epoch / "kept")
    audit = read_jsonl(root / "dedup" / epoch / "input")
    counts = read_jsonl(root / "dedup" / epoch / "counts")
    daily = read_jsonl(root / "windows" / epoch / "daily")
    checks = {}
    fields = ["topic", "epoch", "partition", "input", "first_offset", "last_offset"]
    checks["baseline_offsets_counts"] = bag(baseline, fields) == bag(oracle["baseline"], fields)
    identity = [
        "topic",
        "epoch",
        "partition",
        "kafka_offset",
        "record_sha256",
        "entity_id",
        "entity_revision",
    ]
    checks["main_plus_late_equals_input"] = bag(routed, identity) == bag(
        oracle["records"], identity
    )
    checks["late_routes_valid"] = all(r.get("route") in ("main", "late") for r in routed)
    checks["dedup_input_offsets"] = bag(audit, identity) == bag(oracle["records"], identity)
    unique_fields = ["topic", "epoch", "record_sha256", "entity_id", "entity_revision"]
    expected_unique = Counter({key: 1 for key in bag(oracle["records"], unique_fields)})
    checks["dedup_keeps_every_revision_once"] = bag(kept, unique_fields) == expected_unique
    # Do not pick a retry offset instead of the first one in each partition/key.
    earliest = {}
    for row in oracle["records"]:
        key = (row["topic"], row["record_sha256"])
        earliest.setdefault(key, row)
    checks["dedup_first_offset"] = bag(kept, identity) == bag(earliest.values(), identity)
    fields = ["topic", "epoch", "input", "dup_removed", "kept", "revisions_kept"]
    checks["dedup_counts"] = bag(counts, fields) == bag(
        [r for r in oracle["dedup"] if r["input"]], fields
    )
    derived = []
    for expected in oracle["dedup"]:
        topic = expected["topic"]
        input_n = sum(r["topic"] == topic for r in audit)
        selected = [r for r in kept if r["topic"] == topic]
        derived.append(
            dict(
                topic=topic,
                epoch=oracle["epoch"],
                input=input_n,
                kept=len(selected),
                dup_removed=input_n - len(selected),
                revisions_kept=len(selected) - len({r["entity_id"] for r in selected}),
            )
        )
    checks["dedup_counts_from_outputs"] = bag(derived, fields) == bag(oracle["dedup"], fields)
    late_actual = Counter(r["topic"] for r in routed if r["route"] == "late")
    proof = late_oracle if late_oracle is not None else oracle
    proof_rows = (
        read_jsonl(root / "late-proof" / epoch / "routes") if late_oracle is not None else routed
    )
    proof_actual = Counter(r["topic"] for r in proof_rows if r.get("route") == "late")
    checks["headers_match_epoch"] = all(
        r.get("epoch") == oracle["epoch"]
        for r in baseline + routed + kept + audit + counts + daily + proof_rows
    )
    checks["late_proof_single_partition"] = all(r["partition"] == 0 for r in proof["baseline"])
    checks["late_proof_epoch"] = proof["epoch"] == oracle["epoch"]
    checks["late_proof_input_exact"] = bag(proof_rows, identity) == bag(proof["records"], identity)
    checks["late_proof_routes_valid"] = all(r.get("route") in ("main", "late") for r in proof_rows)
    checks["late_equals_oracle"] = set(proof_actual) <= {r["topic"] for r in proof["late"]} and all(
        proof_actual[r["topic"]] == r["late"] for r in proof["late"]
    )
    checks["late_proof_has_signal"] = sum(r["late"] > 0 for r in proof["late"]) >= 2

    # The copy must keep exactly the envelope/retry set of the three-partition version.
    def content(rows):
        """Compare the source multiset between two receipts, ignoring broker-assigned partition/offset."""
        return bag(rows, ["epoch", "record_sha256", "entity_id", "entity_revision"])

    checks["late_proof_mirrors_input"] = content(proof["records"]) == content(oracle["records"])
    metrics = ("approved_return_events", "successful_product_refund_events")
    window_rows = [
        dict(topic=r["topic"], epoch=r["epoch"], day=r["day"], metric=m, events=r[m])
        for r in daily
        for m in metrics
        if r[m]
    ]
    fields = ["topic", "epoch", "day", "metric", "events"]
    checks["daily_event_windows"] = bag(window_rows, fields) == bag(oracle["windows"], fields)
    return dict(
        passed=all(checks.values()),
        checks=checks,
        dedup_measured=derived,
        late_measured=dict(late_actual),
        late_proof_measured=dict(proof_actual),
        late_model=proof["late"],
    )


def main():
    """CLI returns a nonzero exit code if any reconciliation fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--flink-output",
        required=True,
        help="Directory corresponding to /opt/flink/output/order_lifecycle",
    )
    parser.add_argument("--oracle", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--late-oracle", help="Separate oracle for the single-partition v2.late.<feed>"
    )
    args = parser.parse_args()
    try:
        result = compare(
            args.flink_output,
            json.loads(Path(args.oracle).read_text()),
            json.loads(Path(args.late_oracle).read_text()) if args.late_oracle else None,
        )
    except (KeyError, TypeError, ValueError) as error:
        result = dict(passed=False, checks={"valid_output_schema": False}, error=str(error))
    with Path(args.output).open("x") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
