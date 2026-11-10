"""Build the SCD2 dimension of Amazon Order status from the Bronze snapshots seen."""

from __future__ import annotations

import argparse
import re
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_jobs.lib.iceberg_session import CATALOG, build_spark
from spark_jobs.lib.order_snapshot_mapping import AMAZON_2026_STATUS, AMAZON_V0_STATUS
from spark_jobs.lib.order_version_key import amazon_order_version_key

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
V0 = T.StructType(
    [
        T.StructField("AmazonOrderId", T.StringType()),
        T.StructField("LastUpdateDate", T.StringType()),
        T.StructField("OrderStatus", T.StringType()),
    ]
)
V2026 = T.StructType(
    [
        T.StructField("orderId", T.StringType()),
        T.StructField("lastUpdatedTime", T.StringType()),
        T.StructField(
            "fulfillment",
            T.StructType(
                [
                    T.StructField("status", T.StringType()),
                ]
            ),
        ),
    ]
)


def status_lookup(mapping: dict[str, str], source):
    """Map original status text using the table used in Silver."""
    pairs = [F.lit(value) for item in mapping.items() for value in item]
    return F.create_map(*pairs)[source]


def main(argv: list[str] | None = None) -> int:
    """Build an effective-dated Amazon order dimension from Bronze history."""
    parser = argparse.ArgumentParser(description="Amazon Order Status SCD2")
    parser.add_argument("--source-table", default="raw_amazon_orders_dp1")
    parser.add_argument("--table", default="dim_amazon_order_dp2")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    for value in (args.source_table, args.table):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Invalid table name: {value!r}")

    spark = build_spark("dim-amazon-order-scd2", shuffle_partitions=64)
    try:
        source = f"{CATALOG}.bronze.{args.source_table}"
        bronze = spark.table(source)
        parsed = bronze.select(
            F.col("export_date"),
            F.from_json("payload", V0).alias("v0"),
            F.from_json("payload", V2026).alias("v2026"),
        )
        is_v0 = F.col("v0.AmazonOrderId").isNotNull()
        observed = parsed.select(
            F.coalesce(F.col("v0.AmazonOrderId"), F.col("v2026.orderId")).alias("order_id"),
            F.to_timestamp(
                F.coalesce(F.col("v0.LastUpdateDate"), F.col("v2026.lastUpdatedTime"))
            ).alias("valid_from_ts"),
            F.when(is_v0, status_lookup(AMAZON_V0_STATUS, F.col("v0.OrderStatus")))
            .otherwise(status_lookup(AMAZON_2026_STATUS, F.col("v2026.fulfillment.status")))
            .alias("order_status"),
            F.when(is_v0, F.col("v0.OrderStatus"))
            .otherwise(F.col("v2026.fulfillment.status"))
            .alias("channel_status"),
            F.col("export_date"),
        )
        bad = (
            observed.filter(
                F.col("order_id").isNull()
                | F.col("valid_from_ts").isNull()
                | F.col("order_status").isNull()
                | F.col("channel_status").isNull()
                | F.col("export_date").isNull()
            )
            .limit(1)
            .count()
        )
        if bad:
            raise ValueError("Amazon Bronze is missing the id, time or status for SCD2")

        versions = observed.groupBy("order_id", "valid_from_ts").agg(
            F.min("export_date").alias("first_observed_export_date"),
            F.min("order_status").alias("order_status"),
            # The same canonical status may be written "Shipped" in v0 and
            # "SHIPPED" in the 2026 schema. Keep the source text of the latest export.
            F.max_by(F.col("channel_status"), F.struct("export_date", "channel_status")).alias(
                "channel_status"
            ),
            F.countDistinct("order_status").alias("_distinct_statuses"),
        )
        conflicts = versions.filter(F.col("_distinct_statuses") != 1).limit(1).count()
        if conflicts:
            raise ValueError("Same Order and change time but conflicting statuses")

        timeline = Window.partitionBy("order_id").orderBy("valid_from_ts")
        dimension = (
            versions.drop("_distinct_statuses")
            .withColumn(
                "order_version_key",
                amazon_order_version_key(F.col("order_id"), F.col("valid_from_ts")),
            )
            .withColumn("valid_to_ts", F.lead("valid_from_ts").over(timeline))
            .withColumn("is_current", F.col("valid_to_ts").isNull())
            .withColumn("source_bronze_table", F.lit(source))
            .withColumn("dim_created_at", F.current_timestamp())
            .withColumn("run_id", F.lit(args.run_id))
        )
        target = f"{CATALOG}.gold.{args.table}"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
        dimension.writeTo(target).partitionedBy(F.days("valid_from_ts")).createOrReplace()
        print(f"Amazon SCD2 written: {target}; run_id={args.run_id}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
