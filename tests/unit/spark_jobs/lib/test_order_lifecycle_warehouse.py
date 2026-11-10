"""Check the DDL contract, publish gate and rollback with a fake DB, no services needed."""

import ast
import importlib.util
import re
from pathlib import Path

import pytest

from spark_jobs.lib.order_lifecycle_warehouse import (
    TABLES,
    cutoff_utc,
    ddl_tables,
    index_statements,
    publish,
    should_publish,
    statements,
)

ROOT = Path(__file__).resolve().parents[4]
SQL = ROOT / "sql/warehouse_lifecycle"


def test_ddl_primary_keys_and_real_gold_columns():
    """All three tables are defined, with PKs and columns from the Gold schema in the repo."""
    tables = ddl_tables(SQL)
    assert set(tables) == set(TABLES)
    for ddl in tables.values():
        assert "PRIMARY KEY (dataset_version, line_id" in ddl
        assert ddl.count("(") == ddl.count(")")
    gold = ast.parse((ROOT / "spark_jobs/lib/order_lifecycle_gold.py").read_text())
    label_ddl = next(
        ast.literal_eval(n.value)
        for n in gold.body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "LABEL_DDL" for t in n.targets)
    )
    assert set(re.findall(r"^\s*(\w+) ", tables["label_return"], re.M)) >= {
        c.strip().split()[0] for c in label_ddl.split(",")
    }
    for col in (
        "currency",
        "ordered_basis_qty",
        "canceled_unshipped_qty",
        "delivered_qty",
        "headline_product_net_minor",
        "product_refund_success_minor",
    ):
        assert re.search(rf"\b{col}\b", tables["fact_order_lines"])
    assert "event_timestamp timestamp" in tables["feat_sku_return_rate_90d"]
    assert "created timestamp" in tables["feat_sku_return_rate_90d"]


def test_index_columns_exist_and_no_v1_drop():
    """Every index/INCLUDE/predicate column exists in the target table; migrations never DROP."""
    tables = ddl_tables(SQL)
    indexes = index_statements(SQL)
    assert len(indexes) == 4
    for index in indexes:
        match = re.fullmatch(
            r"CREATE INDEX (\w+) ON warehouse_lifecycle\.(\w+) \(([^)]+)\)(?: INCLUDE \(([^)]+)\))?(?: WHERE (.+))?",
            index,
        )
        assert match, index
        _, table, keys, included, predicate = match.groups()
        cols = [
            c.strip().split()[0] for c in (keys + "," + (included or "")).split(",") if c.strip()
        ]
        if predicate:
            cols.append(predicate.split()[0])
        for col in cols:
            assert re.search(rf"^\s+{col} ", tables[table], re.M), col
    for file in SQL.glob("*.sql"):
        assert not re.search(r"\bDROP\b", "\n".join(statements(file)), re.I)


@pytest.mark.parametrize(
    "checks, expected",
    [
        ({}, False),
        ({"count": True}, True),
        ({"count": True, "money": False}, False),
        ({"count": 1}, False),
        ({"count": None}, False),
    ],
)
def test_publish_gate(checks, expected):
    """Missing/non-boolean checks or mismatched money are never treated as success."""
    assert should_publish(checks) is expected


def test_failed_check_never_executes_swap():
    """A failed gate stops before the first SQL statement, even when staging is ready."""

    class NoSQL:
        """Detect any attempt to touch the DB after validation has failed."""

        def execute(self, *args):
            """Forbid SQL execution in the failing-check case."""
            pytest.fail("Must not swap when a check fails")

    with pytest.raises(ValueError, match="not swapping"):
        publish(NoSQL(), {"fact_order_lines": "staging"}, "test", SQL, {"money": False})


def test_cutoff_requires_timezone():
    """Keep the same UTC instant and reject timestamps without a time zone."""
    assert cutoff_utc("2026-06-01T07:00:00+07:00") == cutoff_utc("2026-06-01T00:00:00Z")
    with pytest.raises(ValueError):
        cutoff_utc("2026-06-01T00:00:00")


def test_explain_buffers_are_not_double_counted():
    """The root node already sums child buffers; the summary must use the root total."""
    spec = importlib.util.spec_from_file_location(
        "wv2_explain", ROOT / "scripts/evidence/measure_order_lifecycle_warehouse.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    raw = [
        {
            "Plan": {
                "Node Type": "Aggregate",
                "Actual Rows": 3,
                "Shared Read Blocks": 2,
                "Shared Hit Blocks": 8,
                "Plans": [{"Shared Hit Blocks": 8}],
            }
        }
    ]
    sample = module.summarize(raw)
    assert sample["shared_total_blocks"] == 10
    assert sample["rows"] == 3
    assert "generation_id=l.generation_id" in module.queries()["return_rate_at_cutoff"]


def test_dag_is_manual_and_validates_before_export():
    """Check the new DAG structure without importing Airflow in the venv."""
    source = (ROOT / "dags/dp_order_lifecycle_warehouse.py").read_text()
    ast.parse(source)
    assert "schedule=None" in source
    assert "validate_gold >> export_warehouse" in source
    assert 'runtime_groups=("iceberg", "postgres")' in source


def test_channels_match_source_v2_names():
    """Channel names in the five_channels check must match the v2 source names (once mistakenly written 'tiktok')."""
    from generator.generate_order_lifecycle_fixture import CHANNELS as SOURCE_CHANNELS
    from spark_jobs.lib.order_lifecycle_warehouse import CHANNELS

    assert set(SOURCE_CHANNELS) == CHANNELS
