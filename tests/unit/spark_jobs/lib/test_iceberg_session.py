"""Offline checks of the Iceberg catalog config (local MinIO, AWS S3) and manifest reading."""

import ast
import json
from pathlib import Path
from types import SimpleNamespace

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
            p + ".warehouse": "custom",
            p + ".uri": env["LAKEKEEPER_CATALOG_URI"],
        }
    )
    assert catalog_conf(env) == expected


def test_unknown_storage_is_rejected():
    """Only minio (local) and s3 (AWS) are accepted; a typo stops immediately."""
    with pytest.raises(ValueError):
        catalog_conf({"PLATFORM_STORAGE": "typo"})


def manifest_reader():
    """Extract the manifest function to test I/O without installing pyspark in the venv."""
    tree = ast.parse((ROOT / "spark_jobs/lib/order_lifecycle_lakehouse.py").read_text())
    function = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "read_source_manifest"
    )
    module = ast.Module(
        body=[
            ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
            function,
        ],
        type_ignores=[],
    )
    ns = {"json": json, "Path": Path, "re": __import__("re")}
    exec(compile(ast.fix_missing_locations(module), "<manifest-reader>", "exec"), ns)
    return ns["read_source_manifest"]


def test_manifest_local_uses_pathlib(tmp_path):
    """The local branch reads with pathlib, without calling Spark."""
    (tmp_path / "manifest.json").write_text('{"dataset_version":"test"}')
    assert manifest_reader()(None, str(tmp_path)) == {"dataset_version": "test"}


@pytest.mark.parametrize("root", ["file:/tmp/fixture", "s3a://raw/fixture"])
def test_manifest_uri_uses_spark(root):
    """A URI keeps its scheme and uses wholetext on the session passed in."""
    calls = []

    def read_text(path, **kwargs):
        """Fake Spark reader that only records manifest read calls."""
        calls.append((path, kwargs))
        return SimpleNamespace(collect=lambda: [{"value": '{"dataset_version":"test"}'}])

    spark = SimpleNamespace(read=SimpleNamespace(text=read_text))
    assert manifest_reader()(spark, root) == {"dataset_version": "test"}
    assert calls == [(root + "/manifest.json", {"wholetext": True})]
