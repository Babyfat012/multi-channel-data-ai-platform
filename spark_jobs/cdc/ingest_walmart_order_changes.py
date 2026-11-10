"""Read committed Walmart CDC events from Kafka into an Iceberg Bronze table.

Kafka incarnation/topic/partition/offset is the source identity. MERGE makes a rerun safe
without suppressing separate updates to the same purchase order.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.cdc_offsets import (
    capture_kafka_boundary,
    freeze_boundary,
    kafka_read_options,
    load_boundary,
)
from spark_jobs.lib.iceberg_session import CATALOG, build_spark

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse Kafka source and Iceberg target options for CDC ingestion."""
    parser = argparse.ArgumentParser(description="Kafka CDC → Iceberg Bronze")
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    parser.add_argument(
        "--topic", default=os.environ.get("KAFKA_TOPIC", "platform_cdc.walmart.orders")
    )
    parser.add_argument("--table", default="raw_walmart_cdc_v2")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--boundary-dir", required=True)
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.table):
        parser.error(f"Invalid table name: {args.table!r}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Ingest Kafka offsets idempotently into the Walmart CDC Bronze table."""
    args = parse_args(argv)
    run_id = args.run_id
    spark = build_spark("bronze-walmart-cdc", shuffle_partitions=4)
    target = f"{CATALOG}.bronze.{args.table}"
    try:
        if spark.catalog.tableExists(target) and "source_epoch" not in spark.table(target).columns:
            raise ValueError("Legacy CDC table has no source epoch; select a fresh v2 table")
        boundary = freeze_boundary(
            args.boundary_dir, run_id, capture_kafka_boundary(spark, args.bootstrap, args.topic)
        )
        source = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap)
            .options(**kafka_read_options(boundary))
            .load()
        )
        events = source.select(
            F.col("topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("event_json"),
        ).filter(F.col("event_json").isNotNull())
        events = events.select(
            "topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "event_json",
            F.get_json_object("event_json", "$.op").alias("operation"),
            F.coalesce(
                F.get_json_object("event_json", "$.after.purchase_order_id"),
                F.get_json_object("event_json", "$.before.purchase_order_id"),
            ).alias("purchase_order_id"),
            F.get_json_object("event_json", "$.source.lsn").cast("long").alias("source_lsn"),
        ).withColumn("source_epoch", F.lit(boundary["cluster_id"] + "/" + boundary["topic_id"]))
        events = (
            events.withColumn("ingested_at", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id))
            .persist()
        )
        incoming = events.count()
        load_boundary(
            args.boundary_dir, run_id, capture_kafka_boundary(spark, args.bootstrap, args.topic)
        )
        if incoming == 0:
            raise ValueError(f"Topic {args.topic} has no CDC events yet")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
        if spark.catalog.tableExists(target):
            events.createOrReplaceTempView("cdc_updates")
            spark.sql(
                f"MERGE INTO {target} AS target USING cdc_updates AS source "
                "ON target.source_epoch = source.source_epoch "
                "AND target.topic = source.topic "
                "AND target.kafka_partition = source.kafka_partition "
                "AND target.kafka_offset = source.kafka_offset "
                "WHEN NOT MATCHED THEN INSERT *"
            )
        else:
            events.writeTo(target).create()
        total = spark.table(target).count()
        print(f"Kafka={incoming:,}, Bronze={total:,}, table={target}, run_id={run_id}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
