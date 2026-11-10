"""Load Walmart v2 Kafka at frozen offsets into an append-only Bronze log, without MERGEing a snapshot."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from spark_jobs.lib.cdc_offsets import (
    capture_kafka_boundary,
    freeze_boundary,
    kafka_read_options,
    load_boundary,
)
from spark_jobs.lib.order_lifecycle_cdc import TARGET, TOPIC, build_session, write_json


def main(argv=None):
    """Fix the boundary before reading, save the receipt and append events with a composite key."""
    from pyspark.sql import functions as F

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--boundary-dir", required=True)
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--local-warehouse")
    args = parser.parse_args(argv)
    spark = build_session("bronze-cdc-v2", args.local_warehouse)
    document = {"run_id": args.run_id, "status": "failed", "target": TARGET}
    try:
        boundary = freeze_boundary(
            args.boundary_dir, args.run_id, capture_kafka_boundary(spark, args.bootstrap, TOPIC)
        )
        epoch = boundary["cluster_id"] + "/" + boundary["topic_id"]
        document.update(boundary=boundary, source_epoch=epoch, status="started")
        write_json(args.receipt, document)
        source = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap)
            .options(**kafka_read_options(boundary))
            .load()
        )
        events = source.select(
            "topic",
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("value").cast("string").alias("event_json"),
        )
        # Keep invalid/null rows so the validator sees them as invalid, not silently filtered out.
        events = events.select(
            "topic",
            "kafka_partition",
            "kafka_offset",
            "event_json",
            F.get_json_object("event_json", "$.op").alias("op"),
            F.coalesce(
                F.get_json_object("event_json", "$.after.dataset_version"),
                F.get_json_object("event_json", "$.before.dataset_version"),
            ).alias("dataset_version"),
            F.coalesce(
                F.get_json_object("event_json", "$.after.purchase_order_id"),
                F.get_json_object("event_json", "$.before.purchase_order_id"),
            ).alias("purchase_order_id"),
            F.get_json_object("event_json", "$.before.raw_payload").alias("payload_before"),
            F.get_json_object("event_json", "$.after.raw_payload").alias("payload_after"),
            F.get_json_object("event_json", "$.source.lsn").cast("long").alias("source_lsn"),
            F.get_json_object("event_json", "$.source.ts_ms").cast("long").alias("source_ts"),
        )
        events = (
            events.withColumn("source_epoch", F.lit(epoch))
            .withColumn("run_id", F.lit(args.run_id))
            .withColumn("record_sha256", F.sha2("payload_after", 256))
            .persist()
        )
        count = events.count()
        load_boundary(
            args.boundary_dir, args.run_id, capture_kafka_boundary(spark, args.bootstrap, TOPIC)
        )
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
        if spark.catalog.tableExists(TARGET):
            # A retry of the same run after append must re-validate; never append duplicates itself.
            if spark.table(TARGET).filter(F.col("run_id") == args.run_id).limit(1).count():
                document["append_skipped_existing_run"] = True
            else:
                events.writeTo(TARGET).append()
        else:
            events.writeTo(TARGET).create()
        load_boundary(
            args.boundary_dir, args.run_id, capture_kafka_boundary(spark, args.bootstrap, TOPIC)
        )
        document.update(status="passed", kafka_rows=count)
        return 0
    except Exception as exc:
        document.update(status="failed", error=str(exc))
        raise
    finally:
        document["completed_at"] = datetime.now(timezone.utc).isoformat()
        write_json(args.receipt, document)
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
