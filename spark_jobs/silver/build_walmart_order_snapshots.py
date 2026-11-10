"""Silver: `bronze.raw_walmart_orders` -> `silver.stg_walmart_order_lines`.

A separate table, not merged with Amazon: the two Channels have different dedup keys. Merging Channels is Gold's job.
Unlike Amazon: one schema shape, times are epoch milliseconds, and there is a `Delivered` status.
`Delivered` covers both delivered and completed, so the time column is `status_changed_at`.

The dedup key needs `customer_order_id`: `purchase_order_id` once collided between two different Orders.
Design: `docs/data-platform/reference/silver.md`.

Run:
    spark-submit ... silver/build_walmart_order_snapshots.py
    spark-submit ... silver/build_walmart_order_snapshots.py \
        --source-table raw_walmart_orders_smoke --table stg_walmart_order_lines_smoke
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
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_snapshot_mapping import COUNTRY_ALIASES, WALMART_STATUS

BRONZE_NS = "bronze"
SILVER_NS = "silver"
MONEY = "decimal(14,2)"

# Unlike Amazon: times are epoch milliseconds (long), money is a number (double), `lineNumber` and
# quantities are strings. Declaring a wrong type makes `from_json` return NULL without an error.

_CHARGE = StructType(
    [
        StructField("chargeType", StringType()),
        StructField("chargeName", StringType()),
        StructField(
            "chargeAmount",
            StructType(
                [
                    StructField("currency", StringType()),
                    StructField("amount", DoubleType()),
                ]
            ),
        ),
    ]
)

_LINE_STATUS = StructType(
    [
        StructField("status", StringType()),
        StructField(
            "trackingInfo",
            StructType(
                [
                    StructField("shipDateTime", LongType()),
                    StructField(
                        "carrierName",
                        StructType(
                            [
                                StructField("carrier", StringType()),
                            ]
                        ),
                    ),
                    StructField("trackingNumber", StringType()),
                ]
            ),
        ),
    ]
)

_ORDER_LINE = StructType(
    [
        StructField("lineNumber", StringType()),
        StructField(
            "item",
            StructType(
                [
                    StructField("productName", StringType()),
                    StructField("sku", StringType()),
                ]
            ),
        ),
        StructField(
            "charges",
            StructType(
                [
                    StructField("charge", ArrayType(_CHARGE)),
                ]
            ),
        ),
        StructField(
            "orderLineQuantity",
            StructType(
                [
                    StructField("unitOfMeasurement", StringType()),
                    StructField("amount", StringType()),
                ]
            ),
        ),
        StructField("statusDate", LongType()),
        StructField(
            "orderLineStatuses",
            StructType(
                [
                    StructField("orderLineStatus", ArrayType(_LINE_STATUS)),
                ]
            ),
        ),
    ]
)

PAYLOAD_SCHEMA = StructType(
    [
        StructField("purchaseOrderId", StringType()),
        StructField("customerOrderId", StringType()),
        StructField("orderDate", LongType()),
        StructField("buyerId", StringType()),
        StructField("isGuest", BooleanType()),
        StructField(
            "shippingInfo",
            StructType(
                [
                    StructField("estimatedDeliveryDate", LongType()),
                    StructField(
                        "postalAddress",
                        StructType(
                            [
                                StructField("city", StringType()),
                                StructField("postalCode", StringType()),
                                StructField("country", StringType()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        StructField(
            "orderLines",
            StructType(
                [
                    StructField("orderLine", ArrayType(_ORDER_LINE)),
                ]
            ),
        ),
    ]
)
# `customerEmailId`, `orderType` are not declared: Silver drops these two keys.

# Output table column order.
OUTPUT_COLUMNS = [
    # key + grain
    "purchase_order_id",
    "customer_order_id",
    "line_number",
    # Order level
    "created_at",
    "order_status",
    "channel_status",
    "buyer_id",
    "is_guest",
    "estimated_delivery_date",
    "city",
    "postal_code",
    "country",
    # Order Line level
    "sku",
    "product_name",
    "quantity",
    "line_net_amount",
    "currency",
    "status_changed_at",
    "ship_date_time",
    "carrier",
    "tracking_number",
    # provenance from Bronze
    "source_table",
    "fetched_at",
    # Silver-only columns
    "processed_at",
    "run_id",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(description="Silver: Walmart Order Line")
    p.add_argument(
        "--source-table",
        default="raw_walmart_orders",
        help="source Bronze table, in the `bronze` namespace.",
    )
    p.add_argument(
        "--table",
        default="stg_walmart_order_lines",
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


def _ts_millis(col):
    """Epoch milliseconds -> timestamp.

    Uses `timestamp_millis` instead of dividing by 1000: division through `double` shifts milliseconds,
    and a shift at a day boundary is a wrong 30-day label.
    """
    return F.timestamp_millis(col)


def _country(col):
    """Normalize country codes to ISO alpha-2 (`"USA"` -> `"US"`); unknown codes are kept."""
    pairs = [F.lit(x) for kv in COUNTRY_ALIASES.items() for x in kv]
    return F.coalesce(F.create_map(*pairs)[col], col)


def _status(col):
    """Walmart original status -> canonical status; unknown text gives NULL so it can be counted."""
    pairs = [F.lit(x) for kv in WALMART_STATUS.items() for x in kv]
    return F.create_map(*pairs)[col]


def parse_order_lines(bronze: DataFrame) -> DataFrame:
    """Parse `raw_payload` and explode `orderLines.orderLine` into Order Lines.

    Takes `[0]` of `charges.charge` and `orderLineStatuses.orderLineStatus` because the source always produces
    exactly one element. If the source adds other charges (shipping, tax), filter by `chargeType`.
    `chargeAmount` is the amount after discounts, so there is only `line_net_amount`.
    """
    doc = F.from_json(F.col("raw_payload"), PAYLOAD_SCHEMA).alias("d")
    df = bronze.select("raw_payload", "source_table", "fetched_at", doc)
    line = F.col("line")
    charge = line["charges"]["charge"][0]
    line_status = line["orderLineStatuses"]["orderLineStatus"][0]
    return df.select(
        "source_table",
        "fetched_at",
        "d",
        F.explode("d.orderLines.orderLine").alias("line"),
    ).select(
        F.col("d.purchaseOrderId").alias("purchase_order_id"),
        F.col("d.customerOrderId").alias("customer_order_id"),
        line["lineNumber"].cast("int").alias("line_number"),
        _ts_millis(F.col("d.orderDate")).alias("created_at"),
        _status(line_status["status"]).alias("order_status"),
        line_status["status"].alias("channel_status"),
        F.col("d.buyerId").alias("buyer_id"),
        F.col("d.isGuest").alias("is_guest"),
        _ts_millis(F.col("d.shippingInfo.estimatedDeliveryDate")).alias("estimated_delivery_date"),
        F.col("d.shippingInfo.postalAddress.city").alias("city"),
        F.col("d.shippingInfo.postalAddress.postalCode").alias("postal_code"),
        _country(F.col("d.shippingInfo.postalAddress.country")).alias("country"),
        line["item"]["sku"].alias("sku"),
        line["item"]["productName"].alias("product_name"),
        line["orderLineQuantity"]["amount"].cast("int").alias("quantity"),
        charge["chargeAmount"]["amount"].cast(MONEY).alias("line_net_amount"),
        charge["chargeAmount"]["currency"].alias("currency"),
        _ts_millis(line["statusDate"]).alias("status_changed_at"),
        _ts_millis(line_status["trackingInfo"]["shipDateTime"]).alias("ship_date_time"),
        line_status["trackingInfo"]["carrierName"]["carrier"].alias("carrier"),
        line_status["trackingInfo"]["trackingNumber"].alias("tracking_number"),
        F.col("source_table"),
        F.col("fetched_at"),
    )


def dedup(df: DataFrame) -> DataFrame:
    """Keep the latest copy of each `(purchase_order_id, customer_order_id, line_number)`.

    Order: latest `status_changed_at`, ties broken by largest `fetched_at`. Ties happen with identical
    copies; then either copy is correct for the business.
    """
    newest = Window.partitionBy("purchase_order_id", "customer_order_id", "line_number").orderBy(
        F.col("status_changed_at").desc_nulls_last(),
        F.col("fetched_at").desc_nulls_last(),
    )
    return df.withColumn("_rn", F.row_number().over(newest)).filter(F.col("_rn") == 1).drop("_rn")


def write_silver(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Overwrite the Silver table in one commit, partitioned by `days(created_at)` like Amazon."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SILVER_NS}")
    full_name = f"{CATALOG}.{SILVER_NS}.{table}"
    df.writeTo(full_name).partitionedBy(F.days("created_at")).createOrReplace()


def main(argv: list[str] | None = None) -> int:
    """Run the job and print row counts before/after dedup."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    source = f"{CATALOG}.{BRONZE_NS}.{args.source_table}"
    spark = build_spark(f"silver-walmart-{args.table}", args.shuffle_partitions)
    try:
        bronze = spark.table(source)

        print(f"run_id          = {run_id}")
        print(f"source table    = {source}")
        print(f"target table    = {CATALOG}.{SILVER_NS}.{args.table}")

        started = time.monotonic()
        lines = (
            parse_order_lines(bronze)
            .withColumn("processed_at", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id))
            .select(*OUTPUT_COLUMNS)
        )
        before = lines.count()
        write_silver(spark, dedup(lines), args.table)
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
