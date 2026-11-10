"""Bronze: file JSONL của Amazon -> Iceberg `bronze.raw_amazon_orders`.

Không parse, không làm sạch: mỗi dòng file thành một dòng bảng, JSON giữ nguyên ở `payload`,
thêm cột provenance (`source_file`, `export_date`, `ingested_at`, `run_id`). Tên khoá Amazon
đổi giữa kỳ, nên parse để Silver lo. Partition theo `export_date`, khớp thư mục nguồn.

Cờ `--aqe off` để chạy bản baseline có skew (cho ảnh Spark UI "trước"), rồi chạy lại với AQE.

Chạy:
    spark-submit ... bronze/ingest_amazon_order_snapshots.py --sample-days 20 --aqe off \
        --table raw_amazon_orders_sample   # baseline
    spark-submit ... bronze/ingest_amazon_order_snapshots.py --sample-days 20 --aqe on \
        --table raw_amazon_orders_sample   # cùng 20 ngày, đã sửa
    spark-submit ... bronze/ingest_amazon_order_snapshots.py --aqe on   # cả kỳ
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

# Mount read-only; dưới nó là export_date=YYYY-MM-DD/part-*.jsonl.
AMAZON_ROOT = "/data/raw/source-exports/channel=amazon/dataset=orders"

CATALOG = "lakehouse"
NAMESPACE = "bronze"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Đọc tham số dòng lệnh."""
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--run-id",
        default=None,
        help="Mã của lần chạy này, ghi vào mọi dòng. Mặc định sinh uuid4. "
        "Giai đoạn Airflow sẽ truyền run_id thật của DAG vào đây để dò ngược được log.",
    )
    p.add_argument(
        "--sample-days",
        type=int,
        default=0,
        help="Chỉ nạp N ngày RẢI ĐỀU trên toàn dải thay vì cả 180 ngày. Rải đều (không "
        "phải N ngày cuối) vì các ngày cuối to gần bằng nhau, lấy liền nhau sẽ KHÔNG "
        "thấy skew. 0 = nạp hết.",
    )
    p.add_argument(
        "--window-days",
        type=int,
        default=0,
        help="Chỉ nạp N ngày LIỀN NHAU thay vì rải đều. Dùng khi cần chứng minh BẢN TRÙNG: "
        "generator xuất lại một Order trong khoảng overlap_days=3, nên ngày rải đều "
        "(cách nhau 9-10 ngày) không bao giờ chứa cùng một Order và dedup bắt được 0 bản. "
        "Đặt >= 7 để phủ cửa sổ ±3. Loại trừ với --sample-days. 0 = không dùng.",
    )
    p.add_argument(
        "--window-start",
        default=None,
        help="Ngày bắt đầu cửa sổ liền nhau (YYYY-MM-DD). Để trống thì lấy cửa sổ ở GIỮA "
        "dải. Chỉ có nghĩa khi đi cùng --window-days.",
    )
    p.add_argument(
        "--aqe",
        choices=("on", "off"),
        default="on",
        help="Adaptive Query Execution. `off` giữ lại skew để chụp ảnh làm bằng chứng.",
    )
    p.add_argument(
        "--shuffle-partitions",
        type=int,
        default=200,
        help="spark.sql.shuffle.partitions. Giữ 200 (mặc định của Spark) ở baseline.",
    )
    p.add_argument(
        "--table",
        default="raw_amazon_orders",
        help="Tên bảng trong namespace bronze. Lần chạy thử nên dùng tên khác "
        "(ví dụ raw_amazon_orders_sample) để không đè bảng thật.",
    )
    p.add_argument(
        "--source",
        default=AMAZON_ROOT,
        help="Thư mục gốc chứa export_date=*/ của Amazon.",
    )
    return p.parse_args(argv)


# Giới hạn: trùng phần lớn `lib/iceberg_session.build_spark`, giữ riêng vì cần cờ AQE và event log
# (phải đặt trước khi tạo session) và vì số đo trong docs/data-platform/reference/spark.md gắn với cấu hình này.
# Gộp khi đo lại Bronze v1.
def build_spark(aqe: str, shuffle_partitions: int, app_name: str) -> SparkSession:
    """SparkSession nối catalog Iceberg trên Lakekeeper, dữ liệu nằm trên MinIO hoặc S3 thật."""
    if os.environ.get("PLATFORM_STORAGE") == "s3":
        # AWS: dùng chung cấu hình catalog S3 (IAM role của EC2), chỉ thêm cờ AQE/shuffle của job này.
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
        # JSON lặp tên khoá rất nhiều nên zstd nén tốt hơn snappy.
        .config("spark.sql.iceberg.compression-codec", "zstd")
        # Event log để xem lại trên History Server sau khi job xong.
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///spark-events")
        .config("spark.sql.adaptive.enabled", "true" if aqe == "on" else "false")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    )
    return builder.getOrCreate()


def list_export_days(spark: SparkSession, source: str) -> list[str]:
    """Liệt kê các `export_date` có thật dưới `source` (chạy được cả local lẫn s3a://)."""
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
    """Đọc JSONL thành một cột chuỗi `value`, kèm đường dẫn file nguồn."""
    reader = spark.read.option("basePath", source)
    if days is None:
        df = reader.text(source)
    else:
        # Đọc thẳng các thư mục ngày cần; `basePath` giữ `export_date` là cột.
        # Phải truyền một list: `text(p1, p2)` hiểu p2 là tham số `wholetext`.
        df = reader.text([f"{source}/export_date={d}" for d in days])
    return df.select(
        F.col("value").alias("payload"),
        F.input_file_name().alias("source_file"),
        F.col("export_date"),
    )


def add_provenance(df: DataFrame, run_id: str) -> DataFrame:
    """Thêm giờ ghi (lúc bắt đầu query, không phải lúc commit) và mã lần chạy."""
    return df.withColumn("ingested_at", F.current_timestamp()).withColumn("run_id", F.lit(run_id))


def write_bronze(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Ghi đè bảng Iceberg trong một commit, partition theo `export_date`.

    Nếu mất ACK lúc commit, kiểm trạng thái bảng trước khi chạy lại.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    full_name = f"{CATALOG}.{NAMESPACE}.{table}"
    df.writeTo(full_name).partitionedBy("export_date").createOrReplace()


def main(argv: list[str] | None = None) -> int:
    """Chạy job và in số dòng để đối chiếu với `run_manifest.json` của generator."""
    args = parse_args(argv)
    run_id = args.run_id or str(uuid.uuid4())
    app_name = f"bronze-amazon-aqe-{args.aqe}-{args.table}"

    spark = build_spark(args.aqe, args.shuffle_partitions, app_name)
    try:
        all_days = list_export_days(spark, args.source)
        if not all_days:
            print(f"KHÔNG tìm thấy export_date=* nào dưới {args.source}", file=sys.stderr)
            return 1
        if args.sample_days > 0 and args.window_days > 0:
            print(
                "--sample-days và --window-days loại trừ nhau: một cái rải đều để thấy "
                "skew, một cái liền nhau để thấy bản trùng. Chọn MỘT.",
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
        print(f"bảng đích       = {CATALOG}.{NAMESPACE}.{args.table}")
        print(f"AQE             = {args.aqe}")
        print(f"ngày có trên đĩa= {len(all_days)}")
        print(f"ngày sẽ nạp     = {len(days) if days else len(all_days)}")
        if days:
            cach = "liền nhau" if args.window_days > 0 else "rải đều"
            print(f"  -> {days[0]} ... {days[-1]} ({cach})")

        started = time.monotonic()
        df = add_provenance(read_raw(spark, args.source, days), run_id)
        write_bronze(spark, df, args.table)
        elapsed = time.monotonic() - started

        rows = spark.table(f"{CATALOG}.{NAMESPACE}.{args.table}").count()
        print(f"XONG sau {elapsed:.1f}s — {rows:,} dòng đã ghi")
        print("Spark UI của lần chạy này đã kết thúc; xem history ở cổng 8080 nếu bật.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
