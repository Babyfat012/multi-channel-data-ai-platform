"""The S3 branch of both Bronze jobs must take its config from catalog_conf, with no MinIO endpoint leaking in."""

import sys
from types import ModuleType, SimpleNamespace

import pytest

# The test venv has no pyspark: build a fake module just good enough for both jobs to import.
_sql = ModuleType("pyspark.sql")
_sql.SparkSession = SimpleNamespace(builder=None)
_sql.DataFrame = object
_sql.functions = ModuleType("pyspark.sql.functions")
sys.modules.setdefault("pyspark", ModuleType("pyspark"))
sys.modules.setdefault("pyspark.sql", _sql)
sys.modules.setdefault("pyspark.sql.functions", _sql.functions)

from spark_jobs.bronze import ingest_amazon_order_snapshots as amazon  # noqa: E402
from spark_jobs.bronze import ingest_walmart_order_snapshots as walmart  # noqa: E402
from spark_jobs.bronze import validate_order_snapshots as validate  # noqa: E402


class FakeBuilder:
    """Record every config instead of starting a JVM."""

    def __init__(self):
        """Start with an empty config."""
        self.conf = {}

    def appName(self, _name):
        """Ignore the app name."""
        return self

    def config(self, key, value):
        """Record one config pair."""
        self.conf[key] = value
        return self

    def getOrCreate(self):
        """Return the config dict instead of a session."""
        return self.conf


@pytest.mark.parametrize(
    "build",
    [lambda: amazon.build_spark("on", 200, "t"), lambda: walmart.build_spark("on", "t")],
    ids=["amazon", "walmart"],
)
def test_s3_branch_uses_catalog_conf(monkeypatch, build):
    """PLATFORM_STORAGE=s3 => region-based s3a endpoint, with no MinIO config left."""
    monkeypatch.setenv("PLATFORM_STORAGE", "s3")
    monkeypatch.setenv("AWS_REGION", "ap-southeast-2")
    for mod in (amazon, walmart):
        monkeypatch.setattr(mod.SparkSession, "builder", FakeBuilder())
    conf = build()
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "s3.ap-southeast-2.amazonaws.com"
    assert conf["spark.sql.catalog.lakehouse.client.region"] == "ap-southeast-2"
    assert conf["spark.sql.adaptive.enabled"] == "true"
    assert not any("minio" in str(v) for v in conf.values())


def test_validate_reads_amazon_source_from_flag():
    """Validate Amazon counts exactly the source passed in (S3 on AWS); the default is still the local mount."""
    assert validate.parse_args(["--channel", "amazon", "--table", "t", "--run-id", "r"]).source == (
        validate.AMAZON_SOURCE
    )
    s3 = "s3a://raw/order_snapshots/source-exports/channel=amazon/dataset=orders"
    args = validate.parse_args(
        ["--channel", "amazon", "--table", "t", "--run-id", "r", "--source", s3]
    )
    read = []
    spark = SimpleNamespace(
        read=SimpleNamespace(text=lambda p: read.append(p) or SimpleNamespace(count=lambda: 7))
    )
    assert validate.amazon_source_rows(spark, 0, None, args.source) == 7
    assert read == [s3]
