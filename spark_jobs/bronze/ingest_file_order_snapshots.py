"""Load TikTok Shop, Shopify or Shopee JSONL sources into Bronze Iceberg.

Each source line is a simulated API response. Bronze keeps the JSON string as is,
without parsing or dedup; it only adds the file path, Export Date, load time and run_id.
Amazon keeps its own job because it is the dataset's measured baseline/AQE evidence.
"""

from __future__ import annotations

import argparse
import re
import sys
import uuid

from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import CATALOG, build_spark

CHANNELS = ("tiktok_shop", "shopify", "shopee")
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the Channel, source path and target Bronze table."""
    parser = argparse.ArgumentParser(description="Bronze JSONL for the three new Channels")
    parser.add_argument("--channel", choices=CHANNELS, required=True)
    parser.add_argument(
        "--source-root",
        required=True,
        help="source-exports/ root containing channel=... directories",
    )
    parser.add_argument("--table", required=True)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(argv)
    if not IDENTIFIER.fullmatch(args.table):
        parser.error(f"Invalid table name: {args.table!r}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Read JSONL and write Bronze; print the row count of the new snapshot."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    source = f"{args.source_root.rstrip('/')}/channel={args.channel}/dataset=orders"
    target = f"{CATALOG}.bronze.{args.table}"
    spark = build_spark(f"bronze-{args.channel}")
    try:
        bronze = (
            spark.read.option("basePath", source)
            .text(source)
            .select(
                F.col("value").alias("payload"),
                F.input_file_name().alias("source_file"),
                F.col("export_date"),
            )
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id))
        )
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
        bronze.writeTo(target).partitionedBy("export_date").createOrReplace()
        rows = spark.table(target).count()
        print(f"source       = {source}")
        print(f"Bronze table = {target}")
        print(f"run_id       = {run_id}")
        print(f"rows loaded  = {rows:,}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
