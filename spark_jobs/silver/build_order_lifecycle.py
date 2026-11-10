"""Bronze v2 -> per-entity Silver tables (`silver.lifecycle_stg_*`), then reconcile.

The logic and invariants live in `lib/order_lifecycle_lakehouse.py`. Reconciliation runs on the tables JUST WRITTEN (read back
from the catalog), not on in-memory DataFrames. Any non-zero check fails the job.
Quarantine counts do not fail the job. The JSON receipt (checks/info/tables/run) is printed to the log and written to a file if `--receipt` is set.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_lifecycle_lakehouse import build_silver, reconcile
from spark_jobs.lib.receipts import completed_at_utc, write_receipt_schemas

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
NAMESPACE = "silver"
TABLES = (
    "line_commitments",
    "allocations",
    "deliveries",
    "return_events",
    "refunds",
    "listing_versions",
    "stock_operations",
    "stock_snapshots",
    "inbound_events",
    "uncertainties",
    "coverage",
    "line_lifecycle",
    "native_order_lines",
    "coverage_receipts",
    "quarantine_records",
    "observed_evidence",
    "coverage_validity",
)
# Observation history: Gold reads by time known ⇒ partition by observation date, not by created_at.
PARTITION_BY_OBSERVED = ("observed_evidence",)


def main(argv: list[str] | None = None) -> int:
    """Write Silver v2, reconcile on the written tables, fail on any violation."""
    parser = argparse.ArgumentParser(description="Silver for the v2 source")
    parser.add_argument("--bronze-table", default="lifecycle_raw_records")
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--as-of", default=None, help="Use only rows observed before this time (UTC)"
    )
    parser.add_argument("--contracts-dir", default=None)
    parser.add_argument("--receipt", type=Path, default=None)
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.bronze_table):
        parser.error(f"Invalid table name: {args.bronze_table!r}")

    spark = build_spark("silver-order-lifecycle", shuffle_partitions=4)
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        bronze = spark.table(f"{CATALOG}.bronze.{args.bronze_table}")
        silver = build_silver(bronze, as_of=args.as_of, contracts_dir=args.contracts_dir)
        names = {}
        for name in TABLES:
            table_name = (
                "lifecycle_quarantine_records"
                if name == "quarantine_records"
                else f"lifecycle_stg_{name}"
            )
            target = f"{CATALOG}.{NAMESPACE}.{table_name}"
            writer = silver[name].writeTo(target)
            if name in PARTITION_BY_OBSERVED:
                writer = writer.partitionedBy(F.days("source_observed_at"))
            writer.createOrReplace()
            names[name] = target
        # Gold checks Silver was built with exactly `as_of = cutoff` through this table (one row per build).
        info_target = f"{CATALOG}.{NAMESPACE}.lifecycle_stg_build_info"
        build_info = spark.createDataFrame(
            [(args.run_id, args.as_of)], "run_id string, as_of string"
        ).withColumn("built_at", F.current_timestamp())
        build_info.writeTo(info_target).createOrReplace()
        names["build_info"] = info_target
        written = {name: spark.table(target) for name, target in names.items()}
        written["registry"] = silver["registry"]
        written["events"] = silver["events"]
        written["dedup"] = silver["dedup"]
        written["native_records"] = silver["native_records"]
        written["parsed_native_records"] = silver["parsed_native_records"]
        result = reconcile(written)
        receipt = {
            "run_id": args.run_id,
            "as_of": args.as_of,
            "bronze_table": f"{CATALOG}.bronze.{args.bronze_table}",
            "tables": names,
            **result,
        }
        if args.receipt:
            schemas = {names[name]: silver[name].schema.jsonValue() for name in TABLES}
            schemas[info_target] = build_info.schema.jsonValue()
            receipt.update(
                completed_at=completed_at_utc(),
                schema_file=write_receipt_schemas(schemas, args.receipt),
            )
        print("Silver v2 receipt: " + json.dumps(receipt, ensure_ascii=False, sort_keys=True))
        if args.receipt:
            args.receipt.parent.mkdir(parents=True, exist_ok=True)
            args.receipt.write_text(
                json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
            )
        failed = {k: v for k, v in result["checks"].items() if v}
        if failed:
            raise ValueError(f"Silver v2 reconciliation failed: {failed}")
        print("Validate Silver v2: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
