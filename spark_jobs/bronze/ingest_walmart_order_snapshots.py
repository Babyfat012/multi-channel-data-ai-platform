"""Bronze: PostgreSQL `walmart.orders` -> Iceberg `bronze.raw_walmart_orders` via JDBC.

Walmart lives in another department's DB, so it is read via JDBC; Amazon delivers files, so it has its own job.
One table per source: an Iceberg commit is atomic, so one broken source does not drag down the other.

`raw_payload` keeps the JSON string as is; parsing is Silver's job. `fetched_at` (when the source pulled
the Order) is separate from `ingested_at` (when Spark wrote it) so source latency can be measured.

Run (needs the PostgreSQL driver in --packages, see scripts/evidence/measure_order_snapshots_bronze.sh):
    spark-submit --master spark://spark-master:7077 --packages <iceberg,aws,postgresql> \
        /opt/spark_jobs/bronze/ingest_walmart_order_snapshots.py --num-partitions 16
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.lib.iceberg_session import catalog_conf

CATALOG = "lakehouse"
NAMESPACE = "bronze"

# The "other department's" table; the data team does not own it.
SOURCE_TABLE = "walmart.orders"

# Spark JDBC can split work on a DATE column, so no id column has to be added to the source table.
PARTITION_COLUMN = "export_date"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--run-id",
        default=None,
        help="Id of this run, written to every row. Defaults to a generated uuid4. The Airflow "
        "stage passes the DAG's real run_id here so logs can be traced back.",
    )
    p.add_argument(
        "--num-partitions",
        type=int,
        default=16,
        help="Number of parallel JDBC connections. 1 = Spark's default behavior (one task pulls the whole "
        "table). Set 16 so 2 workers x 2 cores stay busy without opening too many "
        "connections to PostgreSQL.",
    )
    p.add_argument(
        "--aqe",
        choices=("on", "off"),
        default="on",
        help="Adaptive Query Execution. Kept so the run is comparable with the Amazon job; note the measurements "
        "from the Amazon job show AQE does NOT fix skew when there is no JOIN.",
    )
    p.add_argument(
        "--table",
        default="raw_walmart_orders",
        help="Table name in the bronze namespace. Trial runs should use a different name "
        "(e.g. raw_walmart_orders_smoke) so the real table is not overwritten.",
    )
    return p.parse_args(argv)


def jdbc_options() -> dict[str, str]:
    """PostgreSQL connection details from environment variables; the host is the `postgres` service in Compose."""
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    return {
        "url": f"jdbc:postgresql://{host}:{port}/{db}",
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def source_query() -> str:
    """Source SQL query, casting `raw_payload::text`.

    JDBC returns JSONB as type `OTHER`, which Spark cannot read. Casting to text also keeps the exact source
    string, which is Bronze's job.
    """
    return f"""(
        SELECT purchase_order_id,
               raw_payload::text AS raw_payload,
               fetched_at,
               {PARTITION_COLUMN}
        FROM {SOURCE_TABLE}
    ) AS src"""


# Limitation: largely duplicates `lib/iceberg_session.build_spark`, kept separate because it needs the AQE flag and event log
# (set before the session is created) and because the measurements in docs/data-platform/reference/spark.md are tied to this config.
# Merge when Bronze v1 is re-measured.
def build_spark(aqe: str, app_name: str) -> SparkSession:
    """SparkSession wired to the Iceberg catalog on Lakekeeper, data on MinIO or real S3."""
    if os.environ.get("PLATFORM_STORAGE") == "s3":
        # AWS: reuse the shared S3 catalog config (EC2 IAM role), only adding this job's AQE flag.
        builder = SparkSession.builder.appName(app_name)
        for key, value in catalog_conf(os.environ).items():
            builder = builder.config(key, value)
        return builder.config(
            "spark.sql.adaptive.enabled", "true" if aqe == "on" else "false"
        ).getOrCreate()
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
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", minio_endpoint)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.region", "local-01")
        .config("spark.sql.iceberg.compression-codec", "zstd")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///spark-events")
        .config("spark.sql.adaptive.enabled", "true" if aqe == "on" else "false")
    )
    return builder.getOrCreate()


def read_bounds(spark: SparkSession) -> tuple[str, str, int]:
    """Ask PostgreSQL for the actual `export_date` range before reading.

    `lowerBound`/`upperBound` do not filter rows, they only split the work; a wrong guess leaves one task
    holding almost the whole table. `upper` adds one day because Spark splits half-open ranges.
    """
    opts = jdbc_options()
    stats = f"""(
        SELECT min({PARTITION_COLUMN})            AS lo,
               max({PARTITION_COLUMN}) + 1        AS hi,
               count(*)                           AS n
        FROM {SOURCE_TABLE}
    ) AS bounds"""
    row = spark.read.format("jdbc").options(**opts, dbtable=stats).load().first()
    return str(row["lo"]), str(row["hi"]), int(row["n"])


def read_jdbc(spark: SparkSession, lower: str, upper: str, num_partitions: int) -> DataFrame:
    """Read the table via JDBC with `num_partitions` parallel connections.

    If any of the four partition options is missing, Spark silently uses a single connection. `fetchsize`
    is set non-zero so the driver does not pull the whole result into RAM.
    """
    opts = jdbc_options()
    return (
        spark.read.format("jdbc")
        .options(**opts)
        .option("dbtable", source_query())
        .option("partitionColumn", PARTITION_COLUMN)
        .option("lowerBound", lower)
        .option("upperBound", upper)
        .option("numPartitions", str(num_partitions))
        .option("fetchsize", "10000")
        .load()
    )


def add_provenance(df: DataFrame, run_id: str) -> DataFrame:
    """Add provenance columns; `source_table` is hard-coded because `input_file_name()` is empty over JDBC."""
    return (
        df.withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
    )


def write_bronze(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Overwrite the Iceberg table in one commit, hidden partition `days(fetched_at)`.

    `partitionedBy` needs `F.days(...)`; `F.expr("days(...)")` is rejected.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    full_name = f"{CATALOG}.{NAMESPACE}.{table}"
    df.writeTo(full_name).partitionedBy(F.days("fetched_at")).createOrReplace()


def main(argv: list[str] | None = None) -> int:
    """Run the job and print the row count to compare with PostgreSQL."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    app_name = f"bronze-walmart-jdbc-{args.num_partitions}p-{args.table}"

    spark = build_spark(args.aqe, app_name)
    try:
        lower, upper, source_rows = read_bounds(spark)
        if source_rows == 0:
            print(
                f"{SOURCE_TABLE} is EMPTY. Load it first with:\n"
                "  uv run python -m generator load-postgres --source /data/raw "
                "--sample-days 20 --dsn postgresql://<user>:<pass>@localhost:5432/sources",
                file=sys.stderr,
            )
            return 1

        print(f"run_id            = {run_id}")
        print(f"source table      = {SOURCE_TABLE} ({source_rows:,} rows)")
        print(f"target table      = {CATALOG}.{NAMESPACE}.{args.table}")
        print(f"partitioned by    = {PARTITION_COLUMN} [{lower} .. {upper})")
        print(f"JDBC connections  = {args.num_partitions}")
        print(f"AQE               = {args.aqe}")

        started = time.monotonic()
        df = add_provenance(read_jdbc(spark, lower, upper, args.num_partitions), run_id)
        write_bronze(spark, df, args.table)
        elapsed = time.monotonic() - started

        written = spark.table(f"{CATALOG}.{NAMESPACE}.{args.table}")
        rows = written.count()
        days = written.select(F.to_date("fetched_at").alias("d")).distinct().count()
        print(f"DONE in {elapsed:.1f}s — {rows:,} rows, {days} fetched_at days")
        if rows != source_rows:
            print(
                f"WARNING: {source_rows - rows:,} rows differ from the source. Bronze must "
                "copy faithfully, a difference is a bug: check before using this table.",
                file=sys.stderr,
            )
            return 1
        print("Row count matches source.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
