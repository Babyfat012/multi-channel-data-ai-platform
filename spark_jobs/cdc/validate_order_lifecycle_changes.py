"""Reconcile CDC v2 Bronze against an independent oracle; any violation gives a non-zero exit."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from spark_jobs.lib.cdc_offsets import load_boundary
from spark_jobs.lib.order_lifecycle_cdc import TARGET, build_session, compare_events, write_json


def main(argv=None):
    """Read every row of the same run, including wrong epochs, then write the receipt before failing."""
    from pyspark.sql import functions as F

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--oracle", required=True)
    parser.add_argument("--ingest-receipt", required=True)
    parser.add_argument("--boundary-dir", required=True)
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--local-warehouse")
    args = parser.parse_args(argv)
    document = {"run_id": args.run_id, "status": "failed"}
    spark = None
    try:
        oracle = json.loads(Path(args.oracle).read_text())
        ingest = json.loads(Path(args.ingest_receipt).read_text())
        if (
            oracle.get("format_version") != 1
            or not oracle.get("complete")
            or not oracle.get("events")
        ):
            raise ValueError("Oracle is empty/not fully committed/in a different format")
        if ingest["run_id"] != args.run_id or ingest["status"] != "passed":
            raise ValueError("Ingest receipt did not succeed or belongs to another run")
        boundary = load_boundary(args.boundary_dir, args.run_id, ingest["boundary"])
        epoch = boundary["cluster_id"] + "/" + boundary["topic_id"]
        spark = build_session("validate-cdc-v2", args.local_warehouse)
        rows = [
            r.asDict() for r in spark.table(TARGET).filter(F.col("run_id") == args.run_id).collect()
        ]
        checks = compare_events(oracle["events"], rows, epoch, boundary)
        document.update(
            checks=checks,
            expected_rows=len(oracle["events"]),
            bronze_rows=len(rows),
            boundary=boundary,
            source_epoch=epoch,
            status="failed" if any(checks.values()) else "passed",
        )
        print(json.dumps(document, ensure_ascii=False))
        return 1 if any(checks.values()) else 0
    except Exception as exc:
        document["error"] = str(exc)
        raise
    finally:
        document["completed_at"] = datetime.now(timezone.utc).isoformat()
        write_json(args.receipt, document)
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
