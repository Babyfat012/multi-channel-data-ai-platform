"""Compacting a small Channel's files: SQL changes the spec day → month + rewrites everything; changed data is blocked."""

import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "repartition",
    Path(__file__).resolve().parents[4] / "spark_jobs/maintenance/repartition_small_channel.py",
)
job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(job)
TABLE = "lakehouse.bronze.raw_shopee_orders_dp1"


def test_evolve_and_rewrite_sql():
    """The new spec is months(column); rewrite every file, not only small files below the threshold."""
    assert job.evolve_sql(TABLE, "export_date") == (
        "ALTER TABLE `lakehouse`.`bronze`.`raw_shopee_orders_dp1` "
        "REPLACE PARTITION FIELD `export_date` WITH months(`export_date`)"
    )
    assert job.rewrite_sql(TABLE) == (
        "CALL `lakehouse`.system.rewrite_data_files(table => 'bronze.raw_shopee_orders_dp1', "
        "options => map('rewrite-all', 'true'))"
    )
    with pytest.raises(ValueError):
        job.rewrite_sql("bronze.t; DROP")


def test_changed_data_is_blocked():
    """Same rows + fingerprint passes; a difference in either raises an error."""
    same = {"rows": 10, "fingerprint": "42"}
    job.check_same(same, dict(same))
    for changed in ({"rows": 9, "fingerprint": "42"}, {"rows": 10, "fingerprint": "41"}):
        with pytest.raises(ValueError):
            job.check_same(same, changed)
