"""Build offline features at Order placement time from the five-Channel Gold fact."""

from __future__ import annotations

import argparse
import re
import sys
import uuid

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def main(argv: list[str] | None = None) -> int:
    """Build point-in-time return features from Gold Order Line history."""
    parser = argparse.ArgumentParser(description="Offline features: prediction at Order placement")
    parser.add_argument("--source-table", default="fact_order_lines_dp2")
    parser.add_argument("--table", default="feat_order_line_return_dp3")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(argv)
    for value in (args.source_table, args.table):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Invalid table name: {value!r}")

    spark = build_spark("feat-order-line-return", shuffle_partitions=32)
    try:
        fact = spark.table(f"{CATALOG}.gold.{args.source_table}")
        quantity = F.col("quantity").cast("decimal(14,2)")
        gross = F.col("line_gross_amount")
        discount = F.col("line_discount_amount")
        created_at = F.col("created_at")
        features = fact.select(
            F.col("order_line_key"),
            created_at.alias("event_timestamp"),
            F.current_timestamp().alias("created"),
            F.col("channel"),
            F.col("seller_sku"),
            F.col("currency"),
            F.col("country"),
            F.col("quantity"),
            F.col("line_net_amount"),
            (F.col("line_net_amount") / quantity).cast("decimal(14,2)").alias("unit_net_amount"),
            F.when(gross > 0, (discount / gross).cast("decimal(8,4)")).alias("discount_fraction"),
            F.hour(created_at).alias("order_hour_utc"),
            F.dayofweek(created_at).alias("order_day_of_week"),
            F.month(created_at).alias("order_month"),
            F.dayofweek(created_at).isin(1, 7).alias("is_weekend"),
            F.lit(args.run_id or str(uuid.uuid4())).alias("run_id"),
        )
        target = f"{CATALOG}.gold.{args.table}"
        features.writeTo(target).partitionedBy(F.days("event_timestamp")).createOrReplace()
        print(f"Offline features written: {target}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
