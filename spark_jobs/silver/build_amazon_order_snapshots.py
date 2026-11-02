"""Silver: `bronze.raw_amazon_orders` -> `silver.stg_amazon_order_lines`.

Amazon đổi schema giữa kỳ: dạng v0 (PascalCase) rồi dạng 2026 (camelCase, lồng sâu hơn).
Job parse mỗi dạng bằng schema riêng, gộp về một bộ cột, nổ thành Order Line rồi dedup.

Phải parse trước rồi mới dedup: cùng một đơn có thể có một bản v0 và một bản 2026, chưa
quy tên khoá về một dạng thì không nhận ra là trùng. Bản trùng nằm ở `export_date` khác
nhau nên dedup cần window toàn cục (một lần shuffle).

Amazon không báo "đã giao", nên `order_status` không bao giờ là `delivered`/`completed`.
Thiết kế: `docs/edai-1/reference/silver.md`.

Chạy:
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

# Schema khai tay thay vì để Spark tự suy: tự suy phải quét dữ liệu, và kết quả đổi theo lô.
# Tiền là chuỗi trong JSON nguồn, ép sang decimal ở bước sau.

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

# Hai nhánh parse phải ra đúng bộ cột này.
OUTPUT_COLUMNS = [
    # khoá + độ hạt
    "order_id",
    "order_item_id",
    # mức Order
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
    # mức Order Line
    "asin",
    "seller_sku",
    "quantity_ordered",
    "quantity_shipped",
    "line_gross_amount",
    "line_discount_amount",
    "line_net_amount",
    # provenance từ Bronze
    "export_date",
    "source_file",
    # cột của riêng Silver
    "schema_form",
    "processed_at",
    "run_id",
]

MONEY = "decimal(14,2)"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Đọc tham số dòng lệnh."""
    p = argparse.ArgumentParser(description="Silver: Amazon Order Line")
    p.add_argument(
        "--source-table",
        default="raw_amazon_orders",
        help="bảng Bronze nguồn, trong namespace `bronze`.",
    )
    p.add_argument(
        "--table",
        default="stg_amazon_order_lines",
        help="bảng Silver đích, trong namespace `silver`.",
    )
    p.add_argument(
        "--shuffle-partitions",
        type=int,
        default=None,
        help="spark.sql.shuffle.partitions. Để trống thì giữ mặc định 200.",
    )
    p.add_argument(
        "--run-id", default=None, help="mã lần chạy ghi vào cột run_id. Để trống thì sinh uuid4."
    )
    return p.parse_args(argv)


def _lookup(table: dict[str, str], col):
    """Tra bảng ánh xạ bằng map literal của Spark; giá trị lạ ra NULL để đếm được."""
    pairs = [F.lit(x) for kv in table.items() for x in kv]
    return F.create_map(*pairs)[col]


def parse_v0(bronze: DataFrame) -> DataFrame:
    """Parse dạng v0 (PascalCase) và nổ `OrderItems` thành Order Line."""
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
    """Parse dạng 2026 về cùng bộ cột với v0.

    Dạng 2026 không có số lượng đã giao; ba cột đó để NULL thay vì suy từ trạng thái.
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
        # `programs` chỉ là `["PRIME"]` hoặc `[]`, nên quy về boolean.
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
    """Quy mã quốc gia về ISO alpha-2; mã lạ giữ nguyên (không có cột gốc đi kèm)."""
    pairs = [F.lit(x) for kv in COUNTRY_ALIASES.items() for x in kv]
    return F.coalesce(F.create_map(*pairs)[col], col)


def split_by_form(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Chia dòng Bronze theo dạng schema, nhìn khoá định danh ở gốc JSON."""
    is_v0 = F.get_json_object(F.col("payload"), "$.AmazonOrderId").isNotNull()
    is_2026 = F.get_json_object(F.col("payload"), "$.orderId").isNotNull()
    return bronze.filter(is_v0), bronze.filter(is_2026)


def dedup(df: DataFrame) -> DataFrame:
    """Giữ bản mới nhất của mỗi `(order_id, order_item_id)`.

    Thứ tự: `last_updated_time` lớn nhất, hoà thì `export_date` lớn nhất. Khoá phải có
    `order_item_id`: chỉ theo `order_id` sẽ xoá nhầm các line khác của cùng đơn.
    """
    newest = Window.partitionBy("order_id", "order_item_id").orderBy(
        F.col("last_updated_time").desc_nulls_last(),
        F.col("export_date").desc_nulls_last(),
    )
    return df.withColumn("_rn", F.row_number().over(newest)).filter(F.col("_rn") == 1).drop("_rn")


def write_silver(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Ghi đè bảng Silver trong một commit, partition theo `days(created_at)`.

    Không partition theo `export_date`: sau dedup nó chỉ là ngày của bản thắng, nên một đơn
    sẽ nhảy partition mỗi lần có bản mới.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SILVER_NS}")
    full_name = f"{CATALOG}.{SILVER_NS}.{table}"
    df.writeTo(full_name).partitionedBy(F.days("created_at")).createOrReplace()


def main(argv: list[str] | None = None) -> int:
    """Chạy job và in số dòng trước/sau dedup."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    source = f"{CATALOG}.{BRONZE_NS}.{args.source_table}"
    spark = build_spark(f"silver-amazon-{args.table}", args.shuffle_partitions)
    try:
        bronze = spark.table(source)
        v0_rows, rows_2026 = split_by_form(bronze)

        print(f"run_id          = {run_id}")
        print(f"bảng nguồn      = {source}")
        print(f"bảng đích       = {CATALOG}.{SILVER_NS}.{args.table}")

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
        print(f"Order Line trước dedup = {before:,}")
        print(f"Order Line sau  dedup  = {after:,}")
        print(f"bản trùng đã bỏ        = {before - after:,}")
        if before:
            print(f"tỉ lệ trùng            = {(before - after) / before:.6f}")
        print(f"XONG sau               = {elapsed:.1f}s")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
