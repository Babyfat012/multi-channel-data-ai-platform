"""Nightly gate: CDC Bronze holds every non-null Walmart change event in Kafka, exactly once.

Flink writes the topic into Bronze continuously (flink_jobs/lakehouse/bronze_streams.sql), committing on each
checkpoint. The gate captures Kafka's end offsets once, waits up to --wait-minutes for Flink to commit that far,
then fails the nightly chain (before Silver) on any missing, extra, duplicate, changed or malformed row.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time

from pyspark.sql import functions as F

from spark_jobs.lib.cdc_offsets import capture_kafka_boundary, gate_failures, kafka_read_options
from spark_jobs.lib.iceberg_session import CATALOG, build_spark

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
KEYS = ["topic", "kafka_partition", "kafka_offset"]


def main(argv: list[str] | None = None) -> int:
    """Compare CDC Bronze rows and unique offsets with the Kafka range captured at start."""
    parser = argparse.ArgumentParser(description="Gate: CDC Bronze complete")
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    parser.add_argument(
        "--topic", default=os.environ.get("KAFKA_TOPIC", "platform_cdc.walmart.orders")
    )
    parser.add_argument("--table", default="raw_walmart_cdc_v2")
    # Flink commits every minute; 10 minutes also covers a job restart from its last checkpoint.
    parser.add_argument("--wait-minutes", type=float, default=10)
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.table):
        parser.error(f"Invalid table name: {args.table!r}")

    spark = build_spark("validate-walmart-cdc", shuffle_partitions=4)
    try:
        boundary = capture_kafka_boundary(spark, args.bootstrap, args.topic)
        table = f"{CATALOG}.bronze.{args.table}"
        source = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap)
            .options(**kafka_read_options(boundary))
            .load()
            .filter(F.col("value").isNotNull())
            .select(
                "topic",
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("value").cast("string").alias("source_json"),
            )
            .persist()
        )
        in_range = F.lit(False)
        for partition, start in boundary["starting_offsets"].items():
            in_range = in_range | (
                (F.col("kafka_partition") == int(partition))
                & (F.col("kafka_offset") >= start)
                & (F.col("kafka_offset") < boundary["ending_offsets"][partition])
            )
        epoch = boundary["cluster_id"] + "/" + boundary["topic_id"]

        def bronze_range():
            """Bronze rows of this Kafka incarnation and range, re-read so new Flink commits are seen."""
            spark.sql(f"REFRESH TABLE {table}")
            return spark.table(table).filter(
                (F.col("source_epoch") == epoch) & (F.col("topic") == args.topic) & in_range
            )

        deadline = time.monotonic() + args.wait_minutes * 60
        while True:
            bronze = bronze_range()
            missing = source.join(bronze.select(*KEYS), KEYS, "left_anti").count()
            if not missing or time.monotonic() > deadline:
                break
            print(f"waiting for Flink to commit {missing:,} more events")
            time.sleep(30)
        source_count = source.count()
        result = bronze.agg(
            F.count("*").alias("rows"),
            F.countDistinct(F.struct(*[F.col(key) for key in KEYS])).alias("unique_offsets"),
            F.sum(
                F.when(
                    F.col("event_json").isNull()
                    | F.col("operation").isNull()
                    | ~F.col("operation").isin("c", "u", "d", "r")
                    | F.col("purchase_order_id").isNull()
                    | F.col("source_lsn").isNull(),
                    1,
                ).otherwise(0)
            ).alias("invalid_rows"),
        ).first()
        extra = bronze.join(source.select(*KEYS), KEYS, "left_anti").count()
        mismatched = (
            source.join(bronze, KEYS)
            .filter(~F.col("source_json").eqNullSafe(F.col("event_json")))
            .count()
        )
        rows = int(result["rows"])
        unique = int(result["unique_offsets"])
        invalid = int(result["invalid_rows"] or 0)
        print(
            f"Kafka={source_count:,}, Bronze={rows:,}, unique offsets={unique:,}, "
            f"missing={missing:,}, extra={extra:,}, payload mismatch={mismatched:,}, invalid={invalid:,}"
        )
        failed = gate_failures(source_count, rows, unique, missing, extra, mismatched, invalid)
        if failed:
            raise ValueError("CDC Bronze gate failed: " + ", ".join(failed))
        print("CDC Bronze gate: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
