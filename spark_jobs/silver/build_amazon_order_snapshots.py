"""Silver: `bronze.raw_amazon_orders` -> `silver.stg_amazon_order_lines`.

Amazon changes schema mid-period: the v0 shape (PascalCase), then the 2026 shape (camelCase, more deeply nested).
The job parses each shape with its own schema, merges them into one column set, explodes into Order Lines, then dedups.

Parsing must come before dedup: the same Order may have one v0 copy and one 2026 copy, and until
key names are normalized to one shape they are not recognized as duplicates. Duplicates sit in different
`export_date` values, so dedup needs a global window (one shuffle).

Amazon does not report "delivered", so `order_status` is never `delivered`/`completed`.
Design: `docs/data-platform/reference/silver.md`.

Run:
    spark-submit ... silver/build_amazon_order_snapshots.py
    spark-submit ... silver/build_amazon_order_snapshots.py \
        --source-table raw_amazon_orders_sample --table stg_amazon_order_lines_sample
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_snapshot_mapping import (
    AMAZON_2026_STATUS,
    AMAZON_FULFILLMENT,
    AMAZON_V0_STATUS,
    COUNTRY_ALIASES,
)

BRONZE_NS = "bronze"
SILVER_NS = "silver"

# Schemas are declared by hand instead of letting Spark infer them: inference scans the data and the result varies by batch.
# Money is a string in the source JSON, cast to decimal in a later step.

_V0_ITEM = StructType(
    [
        StructField("OrderItemId", StringType()),
        StructField("ASIN", StringType()),
        StructField("SellerSKU", StringType()),
        StructField("QuantityOrdered", IntegerType()),
        StructField("QuantityShipped", IntegerType()),
        StructField(
            "ItemPrice",
            StructType(
                [
                    StructField("CurrencyCode", StringType()),
                    StructField("Amount", StringType()),
                ]
            ),
        ),
        StructField(
            "PromotionDiscount",
            StructType(
                [
                    StructField("CurrencyCode", StringType()),
                    StructField("Amount", StringType()),
                ]
            ),
        ),
    ]
)

V0_SCHEMA = StructType(
    [
        StructField("AmazonOrderId", StringType()),
        StructField("PurchaseDate", StringType()),
        StructField("LastUpdateDate", StringType()),
        StructField("OrderStatus", StringType()),
        StructField("FulfillmentChannel", StringType()),
        StructField("SalesChannel", StringType()),
        StructField("MarketplaceId", StringType()),
        StructField(
            "OrderTotal",
            StructType(
                [
                    StructField("CurrencyCode", StringType()),
                    StructField("Amount", StringType()),
                ]
            ),
        ),
        StructField("NumberOfItemsShipped", IntegerType()),
        StructField("NumberOfItemsUnshipped", IntegerType()),
        StructField("IsPrime", BooleanType()),
        StructField("EarliestDeliveryDate", StringType()),
        StructField("LatestDeliveryDate", StringType()),
        StructField(
            "ShippingAddress",
            StructType(
                [
                    StructField("City", StringType()),
                    StructField("PostalCode", StringType()),
                    StructField("CountryCode", StringType()),
                ]
            ),
        ),
        StructField("OrderItems", ArrayType(_V0_ITEM)),
    ]
)

_ITEM_2026 = StructType(
    [
        StructField("orderItemId", StringType()),
        StructField(
            "product",
            StructType(
                [
                    StructField("asin", StringType()),
                    StructField("sellerSku", StringType()),
                ]
            ),
        ),
        StructField("quantityOrdered", IntegerType()),
        StructField(
            "price",
            StructType(
                [
                    StructField("currencyCode", StringType()),
                    StructField("amount", StringType()),
                ]
            ),
        ),
        StructField(
            "promotion",
            StructType(
                [
                    StructField("currencyCode", StringType()),
                    StructField("amount", StringType()),
                ]
            ),
        ),
    ]
)

SCHEMA_2026 = StructType(
    [
        StructField("orderId", StringType()),
        StructField("createdTime", StringType()),
        StructField("lastUpdatedTime", StringType()),
        StructField("programs", ArrayType(StringType())),
        StructField(
            "fulfillment",
            StructType(
                [
                    StructField("status", StringType()),
                    StructField("fulfilledBy", StringType()),
                    StructField(
                        "deliverByWindow",
                        StructType(
                            [
                                StructField("startTime", StringType()),
                                StructField("endTime", StringType()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        StructField(
            "salesChannel",
            StructType(
                [
                    StructField("marketplaceId", StringType()),
                    StructField("channelName", StringType()),
                ]
            ),
        ),
        StructField(
            "proceeds",
            StructType(
                [
                    StructField(
                        "grandTotal",
                        StructType(
                            [
                                StructField("currencyCode", StringType()),
                                StructField("amount", StringType()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        StructField(
            "shippingAddress",
            StructType(
                [
                    StructField("city", StringType()),
                    StructField("postalCode", StringType()),
                    StructField("countryCode", StringType()),
                ]
            ),
        ),
        StructField("orderItems", ArrayType(_ITEM_2026)),
    ]
)

# Both parse branches must produce exactly this column set.
OUTPUT_COLUMNS = [
    # key + grain
    "order_id",
    "order_item_id",
    # Order level
    "created_at",
    "last_updated_time",
    "order_status",
    "channel_status",
    "fulfillment_method",
    "sales_channel",
    "marketplace_id",
    "is_prime",
    "order_total_amount",
    "currency",
    "number_of_items_shipped",
    "number_of_items_unshipped",
    "earliest_delivery_date",
    "latest_delivery_date",
    "city",
    "postal_code",
    "country",
    # Order Line level
    "asin",
    "seller_sku",
    "quantity_ordered",
    "quantity_shipped",
    "line_gross_amount",
    "line_discount_amount",
    "line_net_amount",
    # provenance from Bronze
    "export_date",
    "source_file",
    # Silver-only columns
    "schema_form",
    "processed_at",
    "run_id",
]

MONEY = "decimal(14,2)"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(description="Silver: Amazon Order Line")
    p.add_argument(
        "--source-table",
        default="raw_amazon_orders",
        help="source Bronze table, in the `bronze` namespace.",
    )
    p.add_argument(
        "--table",
        default="stg_amazon_order_lines",
        help="target Silver table, in the `silver` namespace.",
    )
    p.add_argument(
        "--shuffle-partitions",
        type=int,
        default=None,
        help="spark.sql.shuffle.partitions. Empty keeps the default of 200.",
    )
    p.add_argument(
        "--run-id",
        default=None,
        help="run id written to the run_id column. Empty generates a uuid4.",
    )
    return p.parse_args(argv)


def _lookup(table: dict[str, str], col):
    """Look up a mapping table with a Spark literal map; unknown values give NULL so they can be counted."""
    pairs = [F.lit(x) for kv in table.items() for x in kv]
    return F.create_map(*pairs)[col]


def parse_v0(bronze: DataFrame) -> DataFrame:
    """Parse the v0 shape (PascalCase) and explode `OrderItems` into Order Lines."""
    doc = F.from_json(F.col("payload"), V0_SCHEMA).alias("d")
    df = bronze.select("payload", "source_file", "export_date", doc)
    item = F.col("item")
    return df.select(
        "source_file", "export_date", "d", F.explode("d.OrderItems").alias("item")
    ).select(
        F.col("d.AmazonOrderId").alias("order_id"),
        item["OrderItemId"].alias("order_item_id"),
        F.to_timestamp(F.col("d.PurchaseDate")).alias("created_at"),
        F.to_timestamp(F.col("d.LastUpdateDate")).alias("last_updated_time"),
        _lookup(AMAZON_V0_STATUS, F.col("d.OrderStatus")).alias("order_status"),
        F.col("d.OrderStatus").alias("channel_status"),
        _lookup(AMAZON_FULFILLMENT, F.col("d.FulfillmentChannel")).alias("fulfillment_method"),
        F.col("d.SalesChannel").alias("sales_channel"),
        F.col("d.MarketplaceId").alias("marketplace_id"),
        F.col("d.IsPrime").alias("is_prime"),
        F.col("d.OrderTotal.Amount").cast(MONEY).alias("order_total_amount"),
        F.col("d.OrderTotal.CurrencyCode").alias("currency"),
        F.col("d.NumberOfItemsShipped").alias("number_of_items_shipped"),
        F.col("d.NumberOfItemsUnshipped").alias("number_of_items_unshipped"),
        F.to_timestamp(F.col("d.EarliestDeliveryDate")).alias("earliest_delivery_date"),
        F.to_timestamp(F.col("d.LatestDeliveryDate")).alias("latest_delivery_date"),
        F.col("d.ShippingAddress.City").alias("city"),
        F.col("d.ShippingAddress.PostalCode").alias("postal_code"),
        _country(F.col("d.ShippingAddress.CountryCode")).alias("country"),
        item["ASIN"].alias("asin"),
        item["SellerSKU"].alias("seller_sku"),
        item["QuantityOrdered"].alias("quantity_ordered"),
        item["QuantityShipped"].alias("quantity_shipped"),
        item["ItemPrice"]["Amount"].cast(MONEY).alias("line_gross_amount"),
        item["PromotionDiscount"]["Amount"].cast(MONEY).alias("line_discount_amount"),
        (item["ItemPrice"]["Amount"].cast(MONEY) - item["PromotionDiscount"]["Amount"].cast(MONEY))
        .cast(MONEY)
        .alias("line_net_amount"),
        F.col("export_date"),
        F.col("source_file"),
        F.lit("v0").alias("schema_form"),
    )


def parse_2026(bronze: DataFrame) -> DataFrame:
    """Parse the 2026 shape into the same column set as v0.

    The 2026 shape has no shipped quantities; those three columns are NULL instead of inferred from the status.
    """
    doc = F.from_json(F.col("payload"), SCHEMA_2026).alias("d")
    df = bronze.select("payload", "source_file", "export_date", doc)
    item = F.col("item")
    return df.select(
        "source_file", "export_date", "d", F.explode("d.orderItems").alias("item")
    ).select(
        F.col("d.orderId").alias("order_id"),
        item["orderItemId"].alias("order_item_id"),
        F.to_timestamp(F.col("d.createdTime")).alias("created_at"),
        F.to_timestamp(F.col("d.lastUpdatedTime")).alias("last_updated_time"),
        _lookup(AMAZON_2026_STATUS, F.col("d.fulfillment.status")).alias("order_status"),
        F.col("d.fulfillment.status").alias("channel_status"),
        _lookup(AMAZON_FULFILLMENT, F.col("d.fulfillment.fulfilledBy")).alias("fulfillment_method"),
        F.col("d.salesChannel.channelName").alias("sales_channel"),
        F.col("d.salesChannel.marketplaceId").alias("marketplace_id"),
        # `programs` is only `["PRIME"]` or `[]`, so reduce it to a boolean.
        F.array_contains(F.col("d.programs"), "PRIME").alias("is_prime"),
        F.col("d.proceeds.grandTotal.amount").cast(MONEY).alias("order_total_amount"),
        F.col("d.proceeds.grandTotal.currencyCode").alias("currency"),
        F.lit(None).cast("int").alias("number_of_items_shipped"),
        F.lit(None).cast("int").alias("number_of_items_unshipped"),
        F.to_timestamp(F.col("d.fulfillment.deliverByWindow.startTime")).alias(
            "earliest_delivery_date"
        ),
        F.to_timestamp(F.col("d.fulfillment.deliverByWindow.endTime")).alias(
            "latest_delivery_date"
        ),
        F.col("d.shippingAddress.city").alias("city"),
        F.col("d.shippingAddress.postalCode").alias("postal_code"),
        _country(F.col("d.shippingAddress.countryCode")).alias("country"),
        item["product"]["asin"].alias("asin"),
        item["product"]["sellerSku"].alias("seller_sku"),
        item["quantityOrdered"].alias("quantity_ordered"),
        F.lit(None).cast("int").alias("quantity_shipped"),
        item["price"]["amount"].cast(MONEY).alias("line_gross_amount"),
        item["promotion"]["amount"].cast(MONEY).alias("line_discount_amount"),
        (item["price"]["amount"].cast(MONEY) - item["promotion"]["amount"].cast(MONEY))
        .cast(MONEY)
        .alias("line_net_amount"),
        F.col("export_date"),
        F.col("source_file"),
        F.lit("2026").alias("schema_form"),
    )


def _country(col):
    """Normalize country codes to ISO alpha-2; unknown codes are kept (no original column alongside)."""
    pairs = [F.lit(x) for kv in COUNTRY_ALIASES.items() for x in kv]
    return F.coalesce(F.create_map(*pairs)[col], col)


def split_by_form(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split Bronze rows by schema shape, looking at the identity key at the JSON root."""
    is_v0 = F.get_json_object(F.col("payload"), "$.AmazonOrderId").isNotNull()
    is_2026 = F.get_json_object(F.col("payload"), "$.orderId").isNotNull()
    return bronze.filter(is_v0), bronze.filter(is_2026)


def dedup(df: DataFrame) -> DataFrame:
    """Keep the latest copy of each `(order_id, order_item_id)`.

    Order: largest `last_updated_time`, ties broken by largest `export_date`. The key must include
    `order_item_id`: keying only on `order_id` would wrongly delete other lines of the same Order.
    """
    newest = Window.partitionBy("order_id", "order_item_id").orderBy(
        F.col("last_updated_time").desc_nulls_last(),
        F.col("export_date").desc_nulls_last(),
    )
    return df.withColumn("_rn", F.row_number().over(newest)).filter(F.col("_rn") == 1).drop("_rn")


def write_silver(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Overwrite the Silver table in one commit, partitioned by `days(created_at)`.

    Not partitioned by `export_date`: after dedup it is only the date of the winning copy, so an Order
    would jump partitions every time a new copy arrives.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SILVER_NS}")
    full_name = f"{CATALOG}.{SILVER_NS}.{table}"
    df.writeTo(full_name).partitionedBy(F.days("created_at")).createOrReplace()


def main(argv: list[str] | None = None) -> int:
    """Run the job and print row counts before/after dedup."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    source = f"{CATALOG}.{BRONZE_NS}.{args.source_table}"
    spark = build_spark(f"silver-amazon-{args.table}", args.shuffle_partitions)
    try:
        bronze = spark.table(source)
        v0_rows, rows_2026 = split_by_form(bronze)

        print(f"run_id          = {run_id}")
        print(f"source table    = {source}")
        print(f"target table    = {CATALOG}.{SILVER_NS}.{args.table}")

        started = time.monotonic()
        lines = parse_v0(v0_rows).unionByName(parse_2026(rows_2026))
        lines = (
            lines.withColumn("processed_at", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id))
            .select(*OUTPUT_COLUMNS)
        )
        before = lines.count()
        deduped = dedup(lines)
        write_silver(spark, deduped, args.table)
        elapsed = time.monotonic() - started

        after = spark.table(f"{CATALOG}.{SILVER_NS}.{args.table}").count()
        print(f"Order Line before dedup = {before:,}")
        print(f"Order Line after dedup  = {after:,}")
        print(f"duplicates dropped      = {before - after:,}")
        if before:
            print(f"duplicate ratio         = {(before - after) / before:.6f}")
        print(f"DONE in                 = {elapsed:.1f}s")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
