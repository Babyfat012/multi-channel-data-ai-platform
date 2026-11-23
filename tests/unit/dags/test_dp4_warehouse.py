"""Warehouse DAG: all five Channels, 30 days before the Gold A Cutoff; a row count mismatch blocks it."""

from __future__ import annotations

from pathlib import Path

import pytest

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
    assert job.index("CREATE SCHEMA IF NOT EXISTS {SCHEMA}") < job.index('.write.format("jdbc")')


def test_count_mismatches_blocks_short_or_empty_channels():
    """A full match is empty; a JDBC write missing a Channel or Gold empty for a Channel is reported."""
    gold = {c: 10 for c in CHANNELS}
    assert count_mismatches(gold, dict(gold)) == []
    assert count_mismatches(gold, gold | {"shopee": 9}) == ["shopee"]
    assert count_mismatches(gold | {"amazon": 0}, gold | {"amazon": 0}) == ["amazon"]


class DbError(Exception):
    """Driver error carrying a SQLSTATE, like psycopg (sqlstate) / psycopg2 (pgcode)."""

    def __init__(self, sqlstate, field="sqlstate"):
        super().__init__(sqlstate)
        setattr(self, field, sqlstate)


class FakeConn:
    """Counts committed swaps; the swap's DROP raises the queued errors one per attempt."""

    def __init__(self, gold, errors):
        self.gold, self.errors, self.autocommit = gold, list(errors), True
        self.swap_tries = self.commits = self.rollbacks = 0

    def cursor(self):
        conn = self

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql, *args):
                if sql.startswith("DROP TABLE IF EXISTS"):
                    conn.swap_tries += 1
                    if conn.errors:
                        raise conn.errors.pop(0)

            def fetchall(self):
                return list(conn.gold.items())

        return Cursor()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@pytest.mark.parametrize(
    ("errors", "tries", "published"),
    [
        ([], 1, True),
        ([DbError("55P03"), DbError("55P03", "pgcode")], 3, True),  # both drivers' field names
        ([DbError("55P03")] * 3, 3, False),  # gives up after the last attempt
        ([DbError("42P01")], 1, False),  # any other error is not retried
    ],
)
def test_swap_retries_only_lock_timeouts(errors, tries, published):
    """lock_timeout hits are retried up to `attempts`; other errors fail at once; failures roll back."""
    from spark_jobs.warehouse.export_order_snapshot_lines import publish

    gold = {c: 1 for c in CHANNELS}
    conn = FakeConn(gold, errors)
    if published:
        assert publish(conn, "t", "t__load_x", gold, attempts=3, pause=0) == gold
    else:
        with pytest.raises(DbError):
            publish(conn, "t", "t__load_x", gold, attempts=3, pause=0)
    assert conn.swap_tries == tries and conn.commits == int(published)
    assert conn.rollbacks == tries - int(published) and conn.autocommit is True
