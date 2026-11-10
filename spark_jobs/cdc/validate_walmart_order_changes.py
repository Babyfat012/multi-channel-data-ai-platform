"""Validate that CDC Bronze contains every non-null Kafka event once."""

from __future__ import annotations

import argparse
import os
import re
import sys

from pyspark.sql import functions as F

from spark_jobs.lib.cdc_offsets import capture_kafka_boundary, kafka_read_options, load_boundary
from spark_jobs.lib.iceberg_session import CATALOG, build_spark

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def main(argv: list[str] | None = None) -> int:
    """Compare CDC Bronze rows and unique offsets with the Kafka source topic."""
    parser = argparse.ArgumentParser(description="Validate CDC Bronze")
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

    spark = build_spark("validate-walmart-cdc", shuffle_partitions=4)
    try:
        boundary = load_boundary(
            args.boundary_dir,
            args.run_id,
            capture_kafka_boundary(spark, args.bootstrap, args.topic),
        )
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
        )
        bronze = spark.table(f"{CATALOG}.bronze.{args.table}")
        if "source_epoch" not in bronze.columns:
            raise ValueError("Legacy CDC table has no source epoch")
        in_range = F.lit(False)
        for partition, start in boundary["starting_offsets"].items():
            in_range = in_range | (
                (F.col("kafka_partition") == int(partition))
                & (F.col("kafka_offset") >= start)
                & (F.col("kafka_offset") < boundary["ending_offsets"][partition])
            )
        bronze = bronze.filter(
            (F.col("source_epoch") == boundary["cluster_id"] + "/" + boundary["topic_id"])
            & (F.col("topic") == args.topic)
            & in_range
        )
        keys = ["topic", "kafka_partition", "kafka_offset"]
        source_count = source.count()
        result = bronze.agg(
            F.count("*").alias("rows"),
            F.countDistinct(F.struct(*[F.col(key) for key in keys])).alias("unique_offsets"),
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
        missing = source.join(bronze.select(*keys), keys, "left_anti").count()
        extra = bronze.join(source.select(*keys), keys, "left_anti").count()
        mismatched = (
            source.join(bronze, keys)
            .filter(~F.col("source_json").eqNullSafe(F.col("event_json")))
            .count()
        )
        load_boundary(
            args.boundary_dir,
            args.run_id,
            capture_kafka_boundary(spark, args.bootstrap, args.topic),
        )
        rows = int(result["rows"])
        unique = int(result["unique_offsets"])
        invalid = int(result["invalid_rows"] or 0)
        print(
            f"Kafka={source_count:,}, Bronze={rows:,}, unique offsets={unique:,}, "
            f"missing={missing:,}, extra={extra:,}, payload mismatch={mismatched:,}, invalid={invalid:,}"
        )
        if (
            source_count <= 0
            or rows != source_count
            or unique != rows
            or missing
            or extra
            or mismatched
            or invalid
        ):
            raise ValueError("Validate CDC Bronze failed")
        print("Validate CDC Bronze: PASS")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
