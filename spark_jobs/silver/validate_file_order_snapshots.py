"""Validate Silver Order Lines for TikTok Shop, Shopify and Shopee."""

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
REQUIRED = (
    "order_id",
    "order_item_id",
    "created_at",
    "last_updated_time",
    "order_status",
    "currency",
    "seller_sku",
    "quantity",
    "line_net_amount",
    "export_date",
    "source_file",
    "processed_at",
    "run_id",
)
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse source and target identifiers for file-backed Silver validation."""
    parser = argparse.ArgumentParser(description="Validate Silver for the three Batch Channels")
    parser.add_argument("--channel", choices=ID_PATH, required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    for value in (args.source_table, args.table):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Invalid table name: {value!r}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Validate Silver schema, key uniqueness, required values, and run id."""
    args = parse_args(argv)
    source = f"{CATALOG}.bronze.{args.source_table}"
    target = f"{CATALOG}.silver.{args.table}"
    spark = build_spark(f"validate-silver-{args.channel}")
    try:
        bronze = spark.table(source)
        silver = spark.table(target)
        missing = sorted(set(REQUIRED) - set(silver.columns))
        if missing:
            raise ValueError(f"{target} is missing columns: {missing}")
        source_orders = (
            bronze.select(F.get_json_object("payload", ID_PATH[args.channel]).alias("id"))
            .distinct()
            .count()
        )
        bad = F.lit(False)
        for name in REQUIRED:
            bad = bad | F.col(name).isNull()
        bad = bad | (F.col("run_id") != args.run_id)
        bad = bad | (F.col("quantity") <= 0)
        bad = bad | (
            F.col("line_gross_amount") - F.col("line_discount_amount") != F.col("line_net_amount")
        )
        counts = silver.agg(
            F.count("*").alias("rows"),
            F.sum(F.when(bad, 1).otherwise(0)).alias("invalid_rows"),
            F.countDistinct("order_id").alias("orders"),
        ).first()
        duplicates = (
            silver.groupBy("order_id", "order_item_id")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )
        print(f"source Bronze             = {source}")
        print(f"target Silver             = {target}")
        print(f"source Orders unique      = {source_orders:,}")
        print(f"Silver Orders unique      = {counts['orders']:,}")
        print(f"Order Line Silver         = {counts['rows']:,}")
        print(f"invalid rows              = {counts['invalid_rows'] or 0:,}")
        print(f"duplicate Order Line keys = {bool(duplicates)}")
        if (
            not source_orders
            or counts["orders"] != source_orders
            or counts["invalid_rows"]
            or duplicates
        ):
            raise ValueError("Silver validate failed")
        print("Silver validate: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
