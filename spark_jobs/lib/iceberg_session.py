"""SparkSession dùng chung, nối catalog Iceberg trên Lakekeeper, ghim múi giờ UTC.

Ghim UTC là bắt buộc: hàm ngày của Spark theo múi giờ phiên, mà nhãn Return dùng cửa sổ
30 ngày, nên lệch múi giờ là lệch nhãn. Hai job Bronze v1 giữ bản `build_spark` riêng vì
đã có số đo đi kèm trong `docs/edai-1/reference/spark.md`.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Khớp tên catalog trong scripts/setup/setup_lakekeeper.sh.
CATALOG = "lakehouse"


def build_spark(app_name: str, shuffle_partitions: int | None = None) -> SparkSession:
    """SparkSession nối Lakekeeper + MinIO, múi giờ UTC.

    `shuffle_partitions=None` giữ mặc định của Spark (200).
    """
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name).config(
        "spark.executorEnv.PYTHONPATH", os.environ.get("PYTHONPATH", "/opt")
    )
    for key, value in catalog_conf(os.environ).items():
        builder = builder.config(key, value)
    if shuffle_partitions is not None:
        builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    return builder.getOrCreate()


def catalog_conf(env) -> dict[str, str]:
    """Cấu hình catalog dạng dict thuần; giữ nguyên các cặp cấu hình MinIO hiện có."""
    prefix = f"spark.sql.catalog.{CATALOG}"
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": env.get("LAKEKEEPER_CATALOG_URI", "http://lakekeeper:8181/catalog"),
        f"{prefix}.warehouse": env.get("WAREHOUSE_NAME", "platform"),
        "spark.sql.iceberg.compression-codec": "zstd",
        "spark.sql.session.timeZone": "UTC",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": env.get("PLATFORM_SPARK_EVENT_LOG_DIR", "file:///spark-events"),
    }
    storage = env.get("PLATFORM_STORAGE", "minio")
    if storage == "minio":
        conf.update(
            {
                f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                f"{prefix}.s3.endpoint": env.get("MINIO_INTERNAL_ENDPOINT", "http://minio:9000"),
                f"{prefix}.s3.path-style-access": "true",
                f"{prefix}.s3.region": "local-01",
            }
        )
    elif storage == "s3":
        # AWS thật: không endpoint/key; credential tạm lấy từ IAM role của EC2.
        region = env["AWS_REGION"]
        conf.update(
            {
                f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                f"{prefix}.client.region": region,
                # File raw đọc qua s3a:// (hadoop-aws); trỏ endpoint theo region để khỏi bị redirect.
                "spark.hadoop.fs.s3a.endpoint": f"s3.{region}.amazonaws.com",
            }
        )
    else:
        raise ValueError(f"PLATFORM_STORAGE không hỗ trợ: {storage!r}")
    return conf
