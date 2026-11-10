"""v2 source (fixture directory) -> Iceberg `bronze.lifecycle_raw_records`, then check the row count.

One shared table for all feeds, one row per JSONL line, keeping `raw_json` as is; the logic lives in
`lib/order_lifecycle_lakehouse.py`. The expected row count comes from the source set's `manifest.json` (only files with
role `source`), not from the data just read. Overwrites the whole table (atomically) on every run.

With `--walmart-jdbc`, Walmart Orders are NOT taken from the bootstrap file but read via JDBC from PostgreSQL
(another department's DB, exactly the boundary the manifest declares). The set of Order ids read must equal
`native_sources.walmart.snapshot_scope.order_ids`; the `ingestion_boundary` column records file/jdbc.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_lifecycle_lakehouse import (
    bronze_from_jdbc,
    bronze_from_root,
    read_source_manifest,
    snapshot_scope_diff,
)
from spark_jobs.lib.receipts import completed_at_utc, write_receipt_schemas

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
DATASET_VERSION = re.compile(r"dataset-[0-9a-f]{32}\Z")
NAMESPACE = "bronze"


def expected_rows(root: Path | str, spark=None) -> tuple[int, str]:
    """Total source rows and dataset_version from the fixture producer's manifest."""
    manifest = read_source_manifest(spark, str(root))
    rows = sum(f["rows"] for f in manifest["files"] if f["role"] == "source")
    return rows, manifest["dataset_version"]


def read_walmart_jdbc(spark, table: str, dataset_version: str):
    """Read Walmart Orders for exactly this dataset_version via JDBC; return (DataFrame, password-free source label).

    `raw_payload` is stored as TEXT, so `::json` keeps the original string (JSONB would reorder keys).
    `json_build_object` rebuilds exactly the three fields of the bootstrap row.
    """
    if not DATASET_VERSION.fullmatch(dataset_version):
        raise ValueError(f"invalid dataset_version: {dataset_version!r}")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ["POSTGRES_DB"]
    url = f"jdbc:postgresql://{host}:{port}/{db}"
    query = f"""(SELECT dataset_version, purchase_order_id,
        json_build_object('fetched_at', fetched_at, 'purchase_order_id', purchase_order_id,
                          'raw_payload', raw_payload::json)::text AS raw_json
        FROM {table} WHERE dataset_version = '{dataset_version}') AS src"""
    rows = (
        spark.read.format("jdbc")
        .options(
            url=url,
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            driver="org.postgresql.Driver",
            dbtable=query,
            fetchsize="1000",
        )
        .load()
    )
    return rows, f"{url}#{table}"


def main(argv: list[str] | None = None) -> int:
    """Write Bronze v2 and fail if the row count/dataset_version does not match the manifest."""
    parser = argparse.ArgumentParser(description="Bronze for the v2 source")
    parser.add_argument(
        "--root", required=True, help="Directory of one fixture variant (with manifest.json)"
    )
    parser.add_argument("--table", default="lifecycle_raw_records")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--receipt", type=Path, default=None)
    parser.add_argument(
        "--walmart-jdbc",
        default=None,
        metavar="SCHEMA.TABLE",
        help="Read Walmart Orders via JDBC from this table instead of the bootstrap file",
    )
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.table):
        parser.error(f"Invalid table name: {args.table!r}")
    if args.walmart_jdbc and not all(IDENTIFIER.fullmatch(p) for p in args.walmart_jdbc.split(".")):
        parser.error(f"Invalid JDBC table name: {args.walmart_jdbc!r}")
    root = args.root
    spark = build_spark("bronze-order-lifecycle", shuffle_partitions=4)
    try:
        want, dataset_version = expected_rows(root, spark)
        manifest = read_source_manifest(spark, root)
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        target = f"{CATALOG}.{NAMESPACE}.{args.table}"
        bronze = bronze_from_root(spark, str(root), args.run_id)
        if args.walmart_jdbc:
            snapshot = manifest["native_sources"]["walmart"]
            rows, label = read_walmart_jdbc(spark, args.walmart_jdbc, dataset_version)
            rows = rows.cache()
            diff = snapshot_scope_diff(
                [r[0] for r in rows.select("purchase_order_id").collect()],
                snapshot["snapshot_scope"]["order_ids"],
            )
            print(
                f"Walmart JDBC: source={label}, Orders={rows.count():,}, "
                f"missing={len(diff['missing'])}, extra={len(diff['extra'])}, duplicate={len(diff['duplicate'])}"
            )
            if any(diff.values()):
                raise ValueError(f"Walmart JDBC snapshot does not match the manifest: {diff}")
            bronze = bronze.filter(F.col("feed") != "walmart.orders").unionByName(
                bronze_from_jdbc(
                    rows, args.run_id, label, snapshot["snapshot_as_of"][:10], snapshot["schema_id"]
                )
            )
        bronze.writeTo(target).partitionedBy(F.col("feed")).createOrReplace()
        table = spark.table(target)
        got = table.count()
        versions = [r[0] for r in table.select("dataset_version").distinct().collect()]
        bad = table.filter(
            F.col("raw_json").isNull() | (F.col("feed") == "") | (F.col("run_id") != args.run_id)
        ).count()
        boundary = {r[0]: r[1] for r in table.groupBy("ingestion_boundary").count().collect()}
        print(
            f"Bronze v2: manifest={want:,}, Bronze={got:,}, invalid={bad:,}, boundary={boundary}, "
            f"dataset_version={versions}, table={target}, run_id={args.run_id}"
        )
        if got != want or bad or versions != [dataset_version]:
            raise ValueError("Bronze v2 does not match the source manifest")
        print("Validate Bronze v2: PASS")
        if args.receipt:
            receipt = {
                "run_id": args.run_id,
                "dataset_version": dataset_version,
                "tables": {"raw_source_records": target},
                "rows": got,
                "completed_at": completed_at_utc(),
                "schema_file": write_receipt_schemas(
                    {target: bronze.schema.jsonValue()}, args.receipt
                ),
            }
            args.receipt.write_text(
                json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
            )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
