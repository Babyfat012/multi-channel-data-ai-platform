"""Validate row counts, keys, money and label boundaries of the Gold fact."""

from __future__ import annotations

import argparse
import re
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
SOURCES = {
    "amazon": "stg_amazon_order_lines_dp2",
    "walmart": "stg_walmart_order_lines_dp2",
    "tiktok_shop": "stg_tiktok_shop_order_lines_dp2",
    "shopify": "stg_shopify_order_lines_dp2",
    "shopee": "stg_shopee_order_lines_dp2",
}


def parse_args(argv: list[str] | None = None):
    """Parse Gold target and source table mappings for validation."""
    parser = argparse.ArgumentParser(description="Validate Gold fact Order Line")
    parser.add_argument("--table", default="fact_order_lines_dp2")
    parser.add_argument("--run-id", required=True)
    for channel, default in SOURCES.items():
        parser.add_argument(f"--{channel.replace('_', '-')}-source", default=default)
    args = parser.parse_args(argv)
    for value in [args.table, *[getattr(args, f"{c}_source") for c in SOURCES]]:
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Invalid table name: {value!r}")
    return args


def metadata_rows(spark, table: str) -> int:
    """Return the current Iceberg snapshot row count for a table."""
    value = spark.table(f"{table}.files").agg(F.sum("record_count")).first()[0]
    return int(value or 0)


def main(argv: list[str] | None = None) -> int:
    """Validate Gold row counts, required columns, and business-key uniqueness."""
    args = parse_args(argv)
    spark = build_spark("validate-gold-order-lines", shuffle_partitions=32)
    try:
        table = f"{CATALOG}.gold.{args.table}"
        fact = spark.table(table)
        forbidden = {"label", "delivered_at", "label_available_at"}
        if forbidden.intersection(fact.columns):
            raise ValueError("Gold fact contains answer columns or the real delivery time")

        expected = {
            channel: metadata_rows(spark, f"{CATALOG}.silver.{getattr(args, f'{channel}_source')}")
            for channel in SOURCES
        }
        observed = {
            row["channel"]: row["count"] for row in fact.groupBy("channel").count().collect()
        }
        print(f"Silver rows: {expected}; Gold rows: {observed}")
        if observed != expected or metadata_rows(spark, table) != sum(expected.values()):
            raise ValueError("Gold row count does not match the five Silver tables")

        bad = (
            fact.filter(
                F.col("order_line_key").isNull()
                | F.col("channel_order_id").isNull()
                | F.col("channel_line_id").isNull()
                | F.col("created_at").isNull()
                | F.col("seller_sku").isNull()
                | F.col("currency").isNull()
                | F.col("line_net_amount").isNull()
                | F.col("quantity").isNull()
                | (F.col("quantity") <= 0)
                | (F.col("line_net_amount") < 0)
                | ((F.col("channel") == "walmart") & F.col("channel_secondary_order_id").isNull())
                | F.col("run_id").isNull()
                | (F.col("run_id") != args.run_id)
                | ((F.col("channel") == "amazon") & F.col("amazon_order_version_key").isNull())
                | ((F.col("channel") != "amazon") & F.col("amazon_order_version_key").isNotNull())
                | (
                    (F.col("channel") != "walmart")
                    & (
                        F.col("line_gross_amount").isNull()
                        | F.col("line_discount_amount").isNull()
                        | (
                            F.col("line_gross_amount") - F.col("line_discount_amount")
                            != F.col("line_net_amount")
                        )
                    )
                )
            )
            .limit(1)
            .count()
        )
        duplicates = (
            fact.groupBy("order_line_key").count().filter(F.col("count") > 1).limit(1).count()
        )
        print(f"Gold invalid={bad}, duplicate_order_line_key={duplicates}")
        if bad or duplicates:
            raise ValueError("Validate Gold fact failed")
        print("Validate Gold fact: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
