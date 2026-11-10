"""Bronze: PostgreSQL `walmart.orders` -> Iceberg `bronze.raw_walmart_orders` qua JDBC.

Walmart nằm trong DB của phòng ban khác nên đọc qua JDBC; Amazon giao file nên có job riêng.
Mỗi nguồn một bảng: commit Iceberg là nguyên khối, nguồn này hỏng không kéo nguồn kia.

`raw_payload` giữ nguyên chuỗi JSON, parse là việc của Silver. `fetched_at` (giờ nguồn kéo
đơn) tách khỏi `ingested_at` (giờ Spark ghi) để đo được độ trễ nguồn.

Chạy (cần driver PostgreSQL trong --packages, xem scripts/evidence/measure_order_snapshots_bronze.sh):
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

# Bảng của "phòng ban khác", data team không sở hữu.
SOURCE_TABLE = "walmart.orders"

# Spark JDBC chia việc được theo cột DATE, nên không phải thêm cột id vào bảng nguồn.
PARTITION_COLUMN = "export_date"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Đọc tham số dòng lệnh."""
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--run-id",
        default=None,
        help="Mã của lần chạy này, ghi vào mọi dòng. Mặc định sinh uuid4. Giai đoạn "
        "Airflow sẽ truyền run_id thật của DAG vào đây để dò ngược được log.",
    )
    p.add_argument(
        "--num-partitions",
        type=int,
        default=16,
        help="Số kết nối JDBC song song. 1 = hành vi mặc định của Spark (một task kéo cả "
        "bảng). Đặt 16 để 2 worker x 2 core có việc liên tục mà không mở quá nhiều "
        "kết nối tới PostgreSQL.",
    )
    p.add_argument(
        "--aqe",
        choices=("on", "off"),
        default="on",
        help="Adaptive Query Execution. Giữ cờ này để so được với job Amazon; lưu ý số đo "
        "ở job Amazon cho thấy AQE KHÔNG chữa được skew khi không có JOIN.",
    )
    p.add_argument(
        "--table",
        default="raw_walmart_orders",
        help="Tên bảng trong namespace bronze. Lần chạy thử nên dùng tên khác "
        "(ví dụ raw_walmart_orders_smoke) để không đè bảng thật.",
    )
    return p.parse_args(argv)


def jdbc_options() -> dict[str, str]:
    """Thông tin kết nối PostgreSQL từ biến môi trường; host là service `postgres` trong Compose."""
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
    """Câu SQL nguồn, ép `raw_payload::text`.

    JDBC trả JSONB dưới kiểu `OTHER` mà Spark không đọc được. Ép text cũng giữ đúng chuỗi
    nguồn, đúng việc của Bronze.
    """
    return f"""(
        SELECT purchase_order_id,
               raw_payload::text AS raw_payload,
               fetched_at,
               {PARTITION_COLUMN}
        FROM {SOURCE_TABLE}
    ) AS src"""


# Giới hạn: trùng phần lớn `lib/iceberg_session.build_spark`, giữ riêng vì cần cờ AQE và event log
# (phải đặt trước khi tạo session) và vì số đo trong docs/data-platform/reference/spark.md gắn với cấu hình này.
# Gộp khi đo lại Bronze v1.
def build_spark(aqe: str, app_name: str) -> SparkSession:
    """SparkSession nối catalog Iceberg trên Lakekeeper, dữ liệu nằm trên MinIO hoặc S3 thật."""
    if os.environ.get("PLATFORM_STORAGE") == "s3":
        # AWS: dùng chung cấu hình catalog S3 (IAM role của EC2), chỉ thêm cờ AQE của job này.
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
    """Hỏi PostgreSQL dải `export_date` thật trước khi đọc.

    `lowerBound`/`upperBound` không lọc dòng, chỉ dùng để chia việc; đoán sai thì một task
    ôm gần hết bảng. `upper` cộng thêm một ngày vì Spark chia khoảng nửa mở.
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
    """Đọc bảng qua JDBC bằng `num_partitions` kết nối song song.

    Thiếu một trong bốn tuỳ chọn partition thì Spark lặng lẽ dùng một kết nối. `fetchsize`
    đặt khác 0 để driver không kéo cả kết quả vào RAM.
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
    """Thêm cột provenance; `source_table` đặt cứng vì `input_file_name()` qua JDBC trả rỗng."""
    return (
        df.withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
    )


def write_bronze(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Ghi đè bảng Iceberg trong một commit, partition ẩn `days(fetched_at)`.

    `partitionedBy` cần `F.days(...)`; `F.expr("days(...)")` bị từ chối.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    full_name = f"{CATALOG}.{NAMESPACE}.{table}"
    df.writeTo(full_name).partitionedBy(F.days("fetched_at")).createOrReplace()


def main(argv: list[str] | None = None) -> int:
    """Chạy job và in số dòng để đối chiếu với PostgreSQL."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    app_name = f"bronze-walmart-jdbc-{args.num_partitions}p-{args.table}"

    spark = build_spark(args.aqe, app_name)
    try:
        lower, upper, source_rows = read_bounds(spark)
        if source_rows == 0:
            print(
                f"{SOURCE_TABLE} đang RỖNG. Nạp trước bằng:\n"
                "  uv run python -m generator load-postgres --source /data/raw "
                "--sample-days 20 --dsn postgresql://<user>:<pass>@localhost:5432/sources",
                file=sys.stderr,
            )
            return 1

        print(f"run_id            = {run_id}")
        print(f"bảng nguồn        = {SOURCE_TABLE} ({source_rows:,} dòng)")
        print(f"bảng đích         = {CATALOG}.{NAMESPACE}.{args.table}")
        print(f"chia theo          = {PARTITION_COLUMN} [{lower} .. {upper})")
        print(f"số kết nối JDBC   = {args.num_partitions}")
        print(f"AQE               = {args.aqe}")

        started = time.monotonic()
        df = add_provenance(read_jdbc(spark, lower, upper, args.num_partitions), run_id)
        write_bronze(spark, df, args.table)
        elapsed = time.monotonic() - started

        written = spark.table(f"{CATALOG}.{NAMESPACE}.{args.table}")
        rows = written.count()
        days = written.select(F.to_date("fetched_at").alias("d")).distinct().count()
        print(f"XONG sau {elapsed:.1f}s — {rows:,} dòng, {days} ngày fetched_at")
        if rows != source_rows:
            print(
                f"CẢNH BÁO: lệch {source_rows - rows:,} dòng so với nguồn. Bronze phải "
                "chép trung thực, lệch là có lỗi — kiểm lại trước khi dùng bảng này.",
                file=sys.stderr,
            )
            return 1
        print("Số dòng khớp nguồn.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
