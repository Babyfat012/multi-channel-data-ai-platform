"""Tests for the quality report: re-measuring from files must match the answer key in the manifest."""

from __future__ import annotations

import pytest

from generator.generate_order_snapshots import BatchRun
from generator.report import QualityReport, render_markdown, render_text


@pytest.fixture
def report(tiny_config):
    """Report on a small Batch, read from a local directory (no MinIO or PostgreSQL needed)."""
    BatchRun(tiny_config).run()
    return QualityReport(tiny_config.storage.output_root)


def test_duplicate_rate_matches_manifest_and_is_zero_after_dedup(report):
    """Prove the re-measured duplicate rate matches the manifest answer key, and drops to 0 after keeping the latest version per key."""
    for row in report.duplicates().as_dicts():
        assert row["dup_pct_before"] == row["dup_pct_truth"]
        assert row["dup_pct_after"] == 0
        assert row["rows_after"] == row["distinct_keys"]


def test_exported_orders_match_truth_for_rendered_storefronts(report):
    """Prove exported Order counts match the core's true counts for Storefronts that have a renderer."""
    for row in report.storefront_skew().as_dicts():
        if row["orders_exported"] is not None:
            assert row["orders_exported"] == row["orders_truth"]


def test_schema_evolution_nulls_flip_at_switch(report):
    """Prove the `proceeds` and `IsPrime` null patterns flip exactly at the schema change."""
    before, after = sorted(
        report.schema_evolution().as_dicts(), key=lambda r: r["period"], reverse=True
    )
    assert before["pct_null_proceeds"] == 100 and before["pct_null_IsPrime"] == 0
    assert after["pct_null_proceeds"] == 0 and after["pct_null_IsPrime"] == 100


def test_category_shares_sum_to_100(report):
    """Prove Order shares per category add up to exactly 100%."""
    assert sum(r["pct"] for r in report.category_skew().as_dicts()) == pytest.approx(100, abs=0.1)


def test_every_table_renders(report):
    """Prove every report table renders as both text and markdown."""
    for table in report.all_tables():
        assert table.title in render_text(table)
        assert render_markdown(table).startswith(f"#### {table.title}")
