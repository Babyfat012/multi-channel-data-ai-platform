"""Validate Bronze JSONL for TikTok Shop, Shopify and Shopee after loading.

Compares the row count with exactly the source JSONL directory and checks original IDs/provenance on every
row. Duplicates from overlapping exports are valid data in Bronze.
"""

from __future__ import annotations

import argparse
import re
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

ID_PATH = {
    "tiktok_shop": "$.data.orders[0].id",
    "shopify": "$.order.id",
    "shopee": "$.response.order_list[0].order_sn",
}
REQUIRED = {"payload", "source_file", "export_date", "ingested_at", "run_id"}
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the Channel, JSONL source, Bronze table and run_id from Airflow."""
    parser = argparse.ArgumentParser(description="Validate Bronze JSONL")
    parser.add_argument("--channel", choices=ID_PATH, required=True)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.table):
        parser.error(f"Invalid table name: {args.table!r}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Return an error code if the table/columns, row count, IDs or provenance fail."""
    args = parse_args(argv)
    source = f"{args.source_root.rstrip('/')}/channel={args.channel}/dataset=orders"
    target = f"{CATALOG}.bronze.{args.table}"
    spark = build_spark(f"validate-bronze-{args.channel}")
    try:
        source_count = spark.read.text(source).count()
        bronze = spark.table(target)
        missing = sorted(REQUIRED - set(bronze.columns))
        if missing:
            raise ValueError(f"{target} is missing columns: {missing}")
        bad = (
            F.get_json_object(F.col("payload"), ID_PATH[args.channel]).isNull()
            | F.col("source_file").isNull()
            | ~F.col("source_file").contains(f"channel={args.channel}")
            | F.col("export_date").isNull()
            | F.col("ingested_at").isNull()
            | F.col("run_id").isNull()
            | (F.col("run_id") != args.run_id)
        )
        counts = bronze.agg(
            F.count("*").alias("rows"),
            F.sum(F.when(bad, 1).otherwise(0)).alias("invalid_rows"),
        ).first()
        target_count = counts["rows"]
        invalid_count = counts["invalid_rows"] or 0
        print(f"source       = {source}")
        print(f"Bronze table = {target}")
        print(f"source rows  = {source_count:,}")
        print(f"Bronze rows  = {target_count:,}")
        print(f"invalid rows = {invalid_count:,}")
        if source_count == 0 or target_count != source_count or invalid_count:
            raise ValueError("Bronze validate failed")
        print("Bronze validate: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
