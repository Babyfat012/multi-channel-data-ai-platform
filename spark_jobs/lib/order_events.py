"""Overlay the order status events Flink streams into Bronze onto batch-built Silver lines.

Events are order level (id, status, time), with no products: they only update lines a batch export already
built. An order seen only in events (created after the last export) gets no Silver line until the next export.
Rule: per order the event with the latest `event_time` wins, and it replaces the batch status only when it is
strictly newer than the line's `last_updated_time` (a late-arriving old event never overwrites).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG


def load_events(spark: SparkSession, table: str, status_map: dict[str, str]) -> DataFrame | None:
    """Read `bronze.<table>` with the canonical status, or None while Flink has not created it yet."""
    name = f"{CATALOG}.bronze.{table}"
    if not spark.catalog.tableExists(name):
        print(f"WARNING: {name} does not exist; using batch status only")
        return None
    pairs = [F.lit(x) for kv in status_map.items() for x in kv]
    return spark.table(name).select(
        "order_id",
        "event_time",
        F.create_map(*pairs)[F.col("channel_status")].alias("order_status"),
        "channel_status",
        "ingested_at",
        "kafka_partition",
        "kafka_offset",
    )


def overlay_latest_status(lines: DataFrame, events: DataFrame | None) -> DataFrame:
    """Replace order_status, channel_status, last_updated_time with the latest newer event of the order."""
    if events is None:
        return lines
    newest = Window.partitionBy("order_id").orderBy(
        F.col("event_time").desc(), F.col("kafka_partition").desc(), F.col("kafka_offset").desc()
    )
    latest = (
        events.withColumn("_rn", F.row_number().over(newest))
        .filter("_rn = 1")
        .select(
            F.col("order_id").alias("_e_order_id"),
            F.col("event_time").alias("_e_time"),
            F.col("order_status").alias("_e_status"),
            F.col("channel_status").alias("_e_channel_status"),
        )
    )
    joined = lines.join(latest, lines["order_id"] == latest["_e_order_id"], "left")
    newer = F.col("_e_time") > F.col("last_updated_time")
    replaced = {
        "order_status": "_e_status",
        "channel_status": "_e_channel_status",
        "last_updated_time": "_e_time",
    }
    return joined.select(
        *[
            F.when(newer, F.col(replaced[c])).otherwise(F.col(c)).alias(c)
            if c in replaced
            else F.col(c)
            for c in lines.columns
        ]
    )
