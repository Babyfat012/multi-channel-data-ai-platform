"""Tests for the `pg-load/` reading of the PostgreSQL load command (no running PostgreSQL needed)."""

from __future__ import annotations

import json

from generator.generate_order_snapshots import BatchRun
from generator.load_postgres import TABLES, iter_rows
from generator.storage import OutputStore


def test_rows_match_manifest_and_columns(tiny_config):
    """Prove the row counts read match the manifest and every row has the declared column count for its table."""
    manifest = BatchRun(tiny_config).run()
    store = OutputStore(tiny_config.storage.output_root)
    for load in TABLES:
        rows = list(iter_rows(store, load))
        assert len(rows) == manifest["datasets"][load.table]["rows"]
        assert all(len(row) == len(load.columns) for row in rows)


def test_walmart_row_takes_export_date_from_folder(tiny_config):
    """Prove a Walmart row's `export_date` comes from the directory name, and the payload matches `purchase_order_id`."""
    BatchRun(tiny_config).run()
    store = OutputStore(tiny_config.storage.output_root)
    walmart = next(load for load in TABLES if load.table == "walmart.orders")
    path = store.glob(walmart.pattern)[0]
    folder_date = path.split("export_date=")[1][:10]
    po_id, _, export_date, payload = next(iter_rows(store, walmart))
    assert export_date == folder_date
    assert json.loads(payload)["purchaseOrderId"] == po_id


def test_sample_days_limits_walmart_day_count(tiny_config):
    """`sample_days=3` reads only 3 export days, and still reads all of `seller.skus`.

    Runs on real generator output, not made-up paths, so this test also catches
    a change in the directory naming convention.
    """
    BatchRun(tiny_config).run()
    store = OutputStore(tiny_config.storage.output_root)
    walmart = next(load for load in TABLES if load.table == "walmart.orders")
    skus = next(load for load in TABLES if load.table == "seller.skus")

    all_days = {row[2] for row in iter_rows(store, walmart)}
    three = {row[2] for row in iter_rows(store, walmart, sample_days=3)}
    assert len(all_days) > 3, "test data needs more than 3 days to check the filtering"
    assert len(three) == 3
    assert three <= all_days

    assert len(list(iter_rows(store, skus, sample_days=3))) == len(list(iter_rows(store, skus)))
