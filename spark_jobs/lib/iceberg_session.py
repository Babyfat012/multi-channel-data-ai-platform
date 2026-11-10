"""Shared SparkSession wired to the Iceberg catalog on Lakekeeper, pinned to UTC.

Pinning UTC is mandatory: Spark date functions follow the session time zone, and the Return label
uses a 30-day window, so a time zone shift is a label shift. The two Bronze v1 jobs keep their own
`build_spark` because they come with measurements in `docs/data-platform/reference/spark.md`.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Matches the catalog name in scripts/setup/setup_lakekeeper.sh.
CATALOG = "lakehouse"


def build_spark(app_name: str, shuffle_partitions: int | None = None) -> SparkSession:
    """SparkSession wired to Lakekeeper + MinIO, UTC time zone.

    `shuffle_partitions=None` keeps the Spark default (200).
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
    """Catalog config as a plain dict; keeps the existing MinIO config pairs unchanged."""
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
        # Real AWS: no endpoint/key; temporary credentials come from the EC2 IAM role.
        region = env["AWS_REGION"]
        conf.update(
            {
                f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                f"{prefix}.client.region": region,
                # Raw files are read via s3a:// (hadoop-aws); point the endpoint at the region to avoid redirects.
                "spark.hadoop.fs.s3a.endpoint": f"s3.{region}.amazonaws.com",
            }
        )
    else:
        raise ValueError(f"unsupported PLATFORM_STORAGE: {storage!r}")
    return conf
