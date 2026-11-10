"""Merge the five Silver tables into a Gold fact at the grain of one product in an Order.

Carries only information observed from the Channel. Return labels and the real delivered_at
stay outside the fact; DP3 will use a separate ground-truth bridge.
"""

from __future__ import annotations

import argparse
import re
import sys
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_version_key import amazon_order_version_key

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
MONEY = "decimal(14,2)"
SOURCES = {
    "amazon": "stg_amazon_order_lines_dp2",
    "walmart": "stg_walmart_order_lines_dp2",
    "tiktok_shop": "stg_tiktok_shop_order_lines_dp2",
    "shopify": "stg_shopify_order_lines_dp2",
    "shopee": "stg_shopee_order_lines_dp2",
}


def null(type_name: str):
    """Create a typed null expression for fields absent from a source Channel."""
    return F.lit(None).cast(type_name)


def common(df: DataFrame, channel: str, source_table: str) -> DataFrame:
    """Project a Channel Silver frame into the canonical Gold fact schema."""
    if channel == "amazon":
        order_id = F.col("order_id")
        second_id = null("string")
        line_id = F.col("order_item_id")
        sku = F.col("seller_sku")
        product_name = null("string")
        quantity = F.col("quantity_ordered")
        gross = F.col("line_gross_amount")
        discount = F.col("line_discount_amount")
        delivery_start = F.col("earliest_delivery_date")
        delivery_end = F.col("latest_delivery_date")
        updated = F.col("last_updated_time")
        source_date = F.col("export_date")
        source_ref = F.col("source_file")
    elif channel == "walmart":
        order_id = F.col("purchase_order_id")
        second_id = F.col("customer_order_id")
        line_id = F.col("line_number").cast("string")
        sku = F.col("sku")
        product_name = F.col("product_name")
        quantity = F.col("quantity")
        gross = null(MONEY)
        discount = null(MONEY)
        delivery_start = null("timestamp")
        delivery_end = F.col("estimated_delivery_date")
        updated = F.col("status_changed_at")
        source_date = F.to_date("fetched_at")
        source_ref = F.col("source_table")
    else:
        order_id = F.col("order_id")
        second_id = null("string")
        line_id = F.col("order_item_id")
        sku = F.col("seller_sku")
        product_name = F.col("product_name")
        quantity = F.col("quantity")
        gross = F.col("line_gross_amount")
        discount = F.col("line_discount_amount")
        delivery_start = null("timestamp")
        delivery_end = null("timestamp")
        updated = F.col("last_updated_time")
        source_date = F.col("export_date")
        source_ref = F.col("source_file")

    return df.select(
        F.lit(channel).alias("channel"),
        order_id.cast("string").alias("channel_order_id"),
        second_id.cast("string").alias("channel_secondary_order_id"),
        line_id.cast("string").alias("channel_line_id"),
        F.col("created_at"),
        updated.alias("last_updated_time"),
        F.col("order_status"),
        F.col("channel_status"),
        sku.cast("string").alias("seller_sku"),
        product_name.alias("product_name"),
        quantity.cast("int").alias("quantity"),
        gross.cast(MONEY).alias("line_gross_amount"),
        discount.cast(MONEY).alias("line_discount_amount"),
        F.col("line_net_amount").cast(MONEY).alias("line_net_amount"),
        F.col("currency"),
        F.col("city"),
        F.col("postal_code"),
        F.col("country"),
        delivery_start.alias("estimated_delivery_start"),
        delivery_end.alias("estimated_delivery_end"),
        source_date.cast("date").alias("source_export_date"),
        source_ref.cast("string").alias("source_record_ref"),
        F.lit(source_table).alias("source_silver_table"),
        F.col("run_id").alias("source_run_id"),
    )


def parse_args(argv: list[str] | None = None):
    """Parse source table mappings and Gold target/run identifiers."""
    parser = argparse.ArgumentParser(description="Gold Order Line fact for the five Channels")
    parser.add_argument("--table", default="fact_order_lines_dp2")
    parser.add_argument("--run-id", default=None)
    for channel, default in SOURCES.items():
        parser.add_argument(f"--{channel.replace('_', '-')}-source", default=default)
    args = parser.parse_args(argv)
    for value in [args.table, *[getattr(args, f"{c}_source") for c in SOURCES]]:
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Invalid table name: {value!r}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Union normalized Channel rows, deduplicate keys, and write the Gold fact."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    spark = build_spark("gold-order-lines", shuffle_partitions=32)
    try:
        frames = []
        for channel in SOURCES:
            source = getattr(args, f"{channel}_source")
            full_name = f"{CATALOG}.silver.{source}"
            frames.append(common(spark.table(full_name), channel, full_name))
        all_lines = frames[0]
        for frame in frames[1:]:
            all_lines = all_lines.unionByName(frame)

        identity = F.struct(
            F.col("channel"),
            F.col("channel_order_id"),
            F.col("channel_secondary_order_id"),
            F.col("channel_line_id"),
        )
        fact = (
            all_lines.withColumn("order_line_key", F.sha2(F.to_json(identity), 256))
            .withColumn(
                "amazon_order_version_key",
                F.when(
                    F.col("channel") == "amazon",
                    amazon_order_version_key(F.col("channel_order_id"), F.col("last_updated_time")),
                ).otherwise(null("string")),
            )
            .withColumn("gold_created_at", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id))
        )
        target = f"{CATALOG}.gold.{args.table}"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
        fact.writeTo(target).partitionedBy(F.days("created_at")).createOrReplace()
        print(f"Gold fact written: {target}; run_id={run_id}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
