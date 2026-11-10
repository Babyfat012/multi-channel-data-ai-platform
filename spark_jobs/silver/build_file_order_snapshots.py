"""Normalize three Batch JSONL sources into Silver at Order Line grain.

Each Channel keeps its own table. Takes the latest snapshot per pair of original IDs of
Order and Order Line; Export Date is not used as a key because exports overlap.
"""

from __future__ import annotations

import argparse
import re
import sys
import uuid

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

CHANNELS = ("tiktok_shop", "shopify", "shopee")
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
MONEY = "decimal(14,2)"

TIKTOK_ITEM = StructType(
    [
        StructField("id", StringType()),
        StructField("seller_sku", StringType()),
        StructField("product_name", StringType()),
        StructField("quantity", IntegerType()),
        StructField("original_price", StringType()),
        StructField("seller_discount", StringType()),
        StructField("platform_discount", StringType()),
    ]
)
SHOPIFY_ITEM = StructType(
    [
        StructField("id", StringType()),
        StructField("sku", StringType()),
        StructField("title", StringType()),
        StructField("quantity", IntegerType()),
        StructField("price", StringType()),
        StructField("total_discount", StringType()),
    ]
)
SHOPEE_ITEM = StructType(
    [
        StructField("order_item_id", StringType()),
        StructField("model_sku", StringType()),
        StructField("item_name", StringType()),
        StructField("model_quantity_purchased", IntegerType()),
        StructField("model_original_price", IntegerType()),
        StructField("model_discounted_price", IntegerType()),
    ]
)


def j(path: str):
    """Read a nested JSON field without assuming it exists in every Channel."""
    return F.get_json_object(F.col("payload"), path)


def epoch_seconds(col):
    """Convert millisecond or second epoch values to Spark timestamps."""
    return F.to_timestamp(F.from_unixtime(col.cast("long")))


def normalize(bronze: DataFrame, channel: str) -> DataFrame:
    """Parse the needed columns from the original response, then explode the Order Line array."""
    if channel == "tiktok_shop":
        root = "$.data.orders[0]"
        item_path = root + ".line_items"
        item_schema = TIKTOK_ITEM
        created = epoch_seconds(j(root + ".create_time"))
        updated = epoch_seconds(j(root + ".update_time"))
        status = j(root + ".status")
        status_map = {
            "UNPAID": "pending_payment",
            "AWAITING_SHIPMENT": "awaiting_shipment",
            "IN_TRANSIT": "shipped",
            "DELIVERED": "delivered",
            "COMPLETED": "completed",
            "CANCELLED": "cancelled",
        }
        order_id = j(root + ".id")
        city = j(root + ".recipient_address.city")
        postal = j(root + ".recipient_address.postal_code")
        country = j(root + ".recipient_address.region_code")
        currency = j(root + ".payment.currency")
    elif channel == "shopify":
        root = "$.order"
        item_path = root + ".line_items"
        item_schema = SHOPIFY_ITEM
        created = F.to_timestamp(j(root + ".created_at"))
        updated = F.to_timestamp(j(root + ".updated_at"))
        status = F.concat_ws("/", j(root + ".financial_status"), j(root + ".fulfillment_status"))
        status_map = None
        order_id = j(root + ".id")
        city = j(root + ".shipping_address.city")
        postal = j(root + ".shipping_address.zip")
        country = j(root + ".shipping_address.country_code")
        currency = j(root + ".currency")
    else:
        root = "$.response.order_list[0]"
        item_path = root + ".item_list"
        item_schema = SHOPEE_ITEM
        created = epoch_seconds(j(root + ".create_time"))
        updated = epoch_seconds(j(root + ".update_time"))
        status = j(root + ".order_status")
        status_map = {
            "UNPAID": "pending_payment",
            "READY_TO_SHIP": "awaiting_shipment",
            "SHIPPED": "shipped",
            "COMPLETED": "completed",
            "CANCELLED": "cancelled",
        }
        order_id = j(root + ".order_sn")
        city = j(root + ".recipient_address.city")
        postal = j(root + ".recipient_address.zipcode")
        country = j(root + ".region")
        currency = j(root + ".currency")

    if status_map is not None:
        pairs = [F.lit(value) for pair in status_map.items() for value in pair]
        canonical = F.create_map(*pairs)[status]
    else:
        canonical = F.when(j(root + ".cancelled_at").isNotNull(), "cancelled")
        canonical = canonical.when(j(root + ".closed_at").isNotNull(), "completed")
        canonical = canonical.when(j(root + ".fulfillment_status") == "fulfilled", "shipped")
        canonical = canonical.when(j(root + ".financial_status") == "paid", "awaiting_shipment")
        canonical = canonical.otherwise("pending_payment")

    parsed = bronze.select(
        order_id.alias("order_id"),
        created.alias("created_at"),
        updated.alias("last_updated_time"),
        canonical.alias("order_status"),
        status.alias("channel_status"),
        currency.alias("currency"),
        city.alias("city"),
        postal.alias("postal_code"),
        country.alias("country"),
        "export_date",
        "source_file",
        F.from_json(j(item_path), ArrayType(item_schema)).alias("items"),
    )
    lines = parsed.select("*", F.explode("items").alias("item")).drop("items")
    item = F.col("item")
    if channel == "tiktok_shop":
        line_id = item["id"]
        sku = item["seller_sku"]
        name = item["product_name"]
        quantity = item["quantity"]
        gross = item["original_price"].cast(MONEY) * quantity
        discount = (
            item["seller_discount"].cast(MONEY) + item["platform_discount"].cast(MONEY)
        ) * quantity
    elif channel == "shopify":
        line_id = item["id"]
        sku = item["sku"]
        name = item["title"]
        quantity = item["quantity"]
        gross = item["price"].cast(MONEY) * quantity
        discount = item["total_discount"].cast(MONEY)
    else:
        line_id = item["order_item_id"]
        sku = item["model_sku"]
        name = item["item_name"]
        quantity = item["model_quantity_purchased"]
        gross = item["model_original_price"].cast(MONEY) * quantity
        discount = gross - item["model_discounted_price"].cast(MONEY) * quantity

    return lines.select(
        "order_id",
        line_id.alias("order_item_id"),
        "created_at",
        "last_updated_time",
        "order_status",
        "channel_status",
        "currency",
        "city",
        "postal_code",
        "country",
        sku.alias("seller_sku"),
        name.alias("product_name"),
        quantity.alias("quantity"),
        gross.cast(MONEY).alias("line_gross_amount"),
        discount.cast(MONEY).alias("line_discount_amount"),
        (gross - discount).cast(MONEY).alias("line_net_amount"),
        "export_date",
        "source_file",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse Channel, table, and run metadata for the Silver file job."""
    parser = argparse.ArgumentParser(description="Silver Order Line for the three Batch Channels")
    parser.add_argument("--channel", choices=CHANNELS, required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(argv)
    for value in (args.source_table, args.table):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Invalid table name: {value!r}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Read file-backed Bronze, normalize and deduplicate, then write Silver."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    spark = build_spark(f"silver-{args.channel}", shuffle_partitions=16)
    source = f"{CATALOG}.bronze.{args.source_table}"
    target = f"{CATALOG}.silver.{args.table}"
    try:
        bronze = spark.table(source)
        lines = (
            normalize(bronze, args.channel)
            .withColumn("processed_at", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id))
        )
        before = lines.count()
        newest = Window.partitionBy("order_id", "order_item_id").orderBy(
            F.col("last_updated_time").desc_nulls_last(),
            F.col("export_date").desc_nulls_last(),
            F.col("source_file").desc_nulls_last(),
        )
        unique = (
            lines.withColumn("_rn", F.row_number().over(newest))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
        unique.writeTo(target).partitionedBy(F.days("created_at")).createOrReplace()
        after = spark.table(target).count()
        print(f"source Bronze           = {source}")
        print(f"target Silver           = {target}")
        print(f"Order Line before dedup = {before:,}")
        print(f"Order Line after dedup  = {after:,}")
        print(f"duplicates dropped      = {before - after:,}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
