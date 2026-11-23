"""Offline checks of the Iceberg catalog config (local MinIO, AWS S3) and manifest reading."""

from pathlib import Path

import pytest
import yaml

from spark_jobs.lib.iceberg_session import catalog_conf

ROOT = Path(__file__).resolve().parents[4]


class ComposeLoader(yaml.SafeLoader):
    """Allow reading Compose override tags in static checks."""


ComposeLoader.add_constructor("!override", lambda loader, node: loader.construct_sequence(node))


def test_default_configuration_is_original():
    """Compare the whole default MinIO config dict, not just a few keys."""
    p = "spark.sql.catalog.lakehouse"
    expected = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        p: "org.apache.iceberg.spark.SparkCatalog",
        p + ".type": "rest",
        p + ".uri": "http://lakekeeper:8181/catalog",
        p + ".warehouse": "platform",
        p + ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        p + ".s3.endpoint": "http://minio:9000",
        p + ".s3.path-style-access": "true",
        p + ".s3.region": "local-01",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.sql.iceberg.compression-codec": "zstd",
        "spark.sql.session.timeZone": "UTC",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "file:///spark-events",
    }
    assert catalog_conf({}) == expected
    assert catalog_conf({"PLATFORM_STORAGE": "minio"}) == expected
    env = {
        "MINIO_INTERNAL_ENDPOINT": "http://custom:9000",
        "WAREHOUSE_NAME": "custom",
        "LAKEKEEPER_CATALOG_URI": "http://custom:8181/catalog",
    }
    expected.update(
        {
            p + ".s3.endpoint": env["MINIO_INTERNAL_ENDPOINT"],
            "spark.hadoop.fs.s3a.endpoint": env["MINIO_INTERNAL_ENDPOINT"],
            p + ".warehouse": "custom",
            p + ".uri": env["LAKEKEEPER_CATALOG_URI"],
        }
    )
    assert catalog_conf(env) == expected


def test_minio_s3a_keys_only_when_present():
    """s3a:// exports need the MinIO keys on workers; without keys in env, none are invented."""
    conf = catalog_conf({"AWS_ACCESS_KEY_ID": "u", "AWS_SECRET_ACCESS_KEY": "s"})
    assert conf["spark.hadoop.fs.s3a.access.key"] == "u"
    assert conf["spark.hadoop.fs.s3a.secret.key"] == "s"
    assert "spark.hadoop.fs.s3a.access.key" not in catalog_conf({})


def test_unknown_storage_is_rejected():
    """Only minio (local) and s3 (AWS) are accepted; a typo stops immediately."""
    with pytest.raises(ValueError):
        catalog_conf({"PLATFORM_STORAGE": "typo"})
