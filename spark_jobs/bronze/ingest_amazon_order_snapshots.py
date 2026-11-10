"""Bronze: Amazon JSONL files -> Iceberg `bronze.raw_amazon_orders`.

No parsing, no cleaning: each file line becomes one table row, JSON kept as is in `payload`,
plus provenance columns (`source_file`, `export_date`, `ingested_at`, `run_id`). Amazon key names
change mid-period, so parsing is left to Silver. Partitioned by `export_date`, matching the source directories.

The `--aqe off` flag runs the skewed baseline (for the "before" Spark UI screenshot), then rerun with AQE.

Run:
    spark-submit ... bronze/ingest_amazon_order_snapshots.py --sample-days 20 --aqe off \
        --table raw_amazon_orders_sample   # baseline
    spark-submit ... bronze/ingest_amazon_order_snapshots.py --sample-days 20 --aqe on \
        --table raw_amazon_orders_sample   # same 20 days, fixed
    spark-submit ... bronze/ingest_amazon_order_snapshots.py --aqe on   # full period
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.lib.day_selection import pick_sample_days, pick_window_days
from spark_jobs.lib.iceberg_session import catalog_conf

# Read-only mount; below it is export_date=YYYY-MM-DD/part-*.jsonl.
AMAZON_ROOT = "/data/raw/source-exports/channel=amazon/dataset=orders"

CATALOG = "lakehouse"
NAMESPACE = "bronze"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--run-id",
        default=None,
        help="Id of this run, written to every row. Defaults to a generated uuid4. "
        "The Airflow stage passes the DAG's real run_id here so logs can be traced back.",
    )
    p.add_argument(
        "--sample-days",
        type=int,
        default=0,
        help="Load only N days spread EVENLY over the whole range instead of all 180 days. Evenly "
        "(not the last N days) because the last days are nearly equal in size, so consecutive days would NOT "
        "show skew. 0 = load all.",
    )
    p.add_argument(
        "--window-days",
        type=int,
        default=0,
        help="Load only N CONSECUTIVE days instead of evenly spaced. Use when you need to prove DUPLICATES: "
        "the generator re-exports an Order within overlap_days=3, so evenly spaced days "
        "(9-10 days apart) never contain the same Order and dedup catches 0 rows. "
        "Set >= 7 to cover the ±3 window. Mutually exclusive with --sample-days. 0 = unused.",
    )
    p.add_argument(
        "--window-start",
        default=None,
        help="Start day of the consecutive window (YYYY-MM-DD). Empty takes the window in the MIDDLE "
        "of the range. Only meaningful together with --window-days.",
    )
    p.add_argument(
        "--aqe",
        choices=("on", "off"),
        default="on",
        help="Adaptive Query Execution. `off` keeps the skew so it can be captured as evidence.",
    )
    p.add_argument(
        "--shuffle-partitions",
        type=int,
        default=200,
        help="spark.sql.shuffle.partitions. Keep 200 (the Spark default) for the baseline.",
    )
    p.add_argument(
        "--table",
        default="raw_amazon_orders",
        help="Table name in the bronze namespace. Trial runs should use a different name "
        "(e.g. raw_amazon_orders_sample) so the real table is not overwritten.",
    )
    p.add_argument(
        "--source",
        default=AMAZON_ROOT,
        help="Root directory containing Amazon export_date=*/.",
    )
    return p.parse_args(argv)


# Limitation: largely duplicates `lib/iceberg_session.build_spark`, kept separate because it needs the AQE flag and event log
# (set before the session is created) and because the measurements in docs/data-platform/reference/spark.md are tied to this config.
# Merge when Bronze v1 is re-measured.
def build_spark(aqe: str, shuffle_partitions: int, app_name: str) -> SparkSession:
    """SparkSession wired to the Iceberg catalog on Lakekeeper, data on MinIO or real S3."""
    if os.environ.get("PLATFORM_STORAGE") == "s3":
        # AWS: reuse the shared S3 catalog config (EC2 IAM role), only adding this job's AQE/shuffle flags.
        builder = SparkSession.builder.appName(app_name)
        for key, value in catalog_conf(os.environ).items():
            builder = builder.config(key, value)
        return (
            builder.config("spark.sql.adaptive.enabled", "true" if aqe == "on" else "false")
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .getOrCreate()
        )
    minio_endpoint = os.environ.get("MINIO_INTERNAL_ENDPOINT", "http://minio:9000")
    warehouse = os.environ.get("WAREHOUSE_NAME", "platform")
    catalog_uri = os.environ.get("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog")

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", catalog_uri)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config(
            f"spark.sql.catalog.{CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", minio_endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.region", "local-01")
        # JSON repeats key names a lot, so zstd compresses better than snappy.
        .config("spark.sql.iceberg.compression-codec", "zstd")
        # Event log for review on the History Server after the job finishes.
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///spark-events")
        .config("spark.sql.adaptive.enabled", "true" if aqe == "on" else "false")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    )
    return builder.getOrCreate()


def list_export_days(spark: SparkSession, source: str) -> list[str]:
    """List the `export_date` values that actually exist under `source` (works both locally and on s3a://)."""
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(source)
    fs = path.getFileSystem(hadoop_conf)
    days = []
    for status in fs.listStatus(path):
        name = status.getPath().getName()
        if status.isDirectory() and name.startswith("export_date="):
            days.append(name.split("=", 1)[1])
    return sorted(days)


def read_raw(spark: SparkSession, source: str, days: list[str] | None) -> DataFrame:
    """Read JSONL into a single string column `value`, with the source file path."""
    reader = spark.read.option("basePath", source)
    if days is None:
        df = reader.text(source)
    else:
        # Read only the needed day directories; `basePath` keeps `export_date` as a column.
        # Must pass a list: `text(p1, p2)` treats p2 as the `wholetext` parameter.
        df = reader.text([f"{source}/export_date={d}" for d in days])
    return df.select(
        F.col("value").alias("payload"),
        F.input_file_name().alias("source_file"),
        F.col("export_date"),
    )


def add_provenance(df: DataFrame, run_id: str) -> DataFrame:
    """Add the write time (query start, not commit time) and the run id."""
    return df.withColumn("ingested_at", F.current_timestamp()).withColumn("run_id", F.lit(run_id))


def write_bronze(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Overwrite the Iceberg table in one commit, partitioned by `export_date`.

    If the ACK is lost at commit, check the table state before rerunning.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    full_name = f"{CATALOG}.{NAMESPACE}.{table}"
    df.writeTo(full_name).partitionedBy("export_date").createOrReplace()


def main(argv: list[str] | None = None) -> int:
    """Run the job and print the row count to compare with the generator's `run_manifest.json`."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    app_name = f"bronze-amazon-aqe-{args.aqe}-{args.table}"

    spark = build_spark(args.aqe, args.shuffle_partitions, app_name)
    try:
        all_days = list_export_days(spark, args.source)
        if not all_days:
            print(f"NO export_date=* found under {args.source}", file=sys.stderr)
            return 1
        if args.sample_days > 0 and args.window_days > 0:
            print(
                "--sample-days and --window-days are mutually exclusive: one spreads evenly to see "
                "skew, the other is consecutive to see duplicates. Pick ONE.",
                file=sys.stderr,
            )
            return 2
        if args.window_days > 0:
            days = pick_window_days(all_days, args.window_days, args.window_start)
        elif args.sample_days > 0:
            days = pick_sample_days(all_days, args.sample_days)
        else:
            days = None

        print(f"run_id          = {run_id}")
        print(f"target table    = {CATALOG}.{NAMESPACE}.{args.table}")
        print(f"AQE             = {args.aqe}")
        print(f"days on disk    = {len(all_days)}")
        print(f"days to load    = {len(days) if days else len(all_days)}")
        if days:
            spacing = "consecutive" if args.window_days > 0 else "evenly spaced"
            print(f"  -> {days[0]} ... {days[-1]} ({spacing})")

        started = time.monotonic()
        df = add_provenance(read_raw(spark, args.source, days), run_id)
        write_bronze(spark, df, args.table)
        elapsed = time.monotonic() - started

        rows = spark.table(f"{CATALOG}.{NAMESPACE}.{args.table}").count()
        print(f"DONE in {elapsed:.1f}s — {rows:,} rows written")
        print("The Spark UI for this run has ended; see history on port 8080 if enabled.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
