"""Warehouse DAG: all five Channels, 30 days before the Gold A Cutoff; a row count mismatch blocks it."""

from __future__ import annotations

from pathlib import Path

from spark_jobs.lib.label_channels import CHANNELS, count_mismatches

ROOT = Path(__file__).resolve().parents[3]


def test_dag_exports_thirty_days_before_ml_cutoff():
    """Same Cutoff as ML (Gold A), 30-day window, the right warehouse job."""
    text = (ROOT / "dags/dp4_warehouse.py").read_text()
    assert "/opt/spark_jobs/warehouse/export_order_snapshot_lines.py" in text
    assert "var.value.get('platform_ml_cutoff'" in text
    assert '"--days",\n            "30"' in text
    assert (
        'runtime_groups=("iceberg", "postgres")' in text
    )  # without it: KeyError POSTGRES_PASSWORD
    job = (ROOT / "spark_jobs/warehouse/export_order_snapshot_lines.py").read_text()
    assert 'F.col("channel") == "walmart"' not in job
    # JDBC creates only tables, not schemas: the schema must exist before the first write (brand-new AWS Postgres).
    assert job.index("CREATE SCHEMA IF NOT EXISTS warehouse") < job.index('.write.format("jdbc")')


def test_count_mismatches_blocks_short_or_empty_channels():
    """A full match is empty; a JDBC write missing a Channel or Gold empty for a Channel is reported."""
    gold = {c: 10 for c in CHANNELS}
    assert count_mismatches(gold, dict(gold)) == []
    assert count_mismatches(gold, gold | {"shopee": 9}) == ["shopee"]
    assert count_mismatches(gold | {"amazon": 0}, gold | {"amazon": 0}) == ["amazon"]
