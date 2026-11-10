"""Tests running a full small Batch: output files, manifest and label table."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pyarrow.parquet as pq

from generator.generate_order_snapshots import BatchRun


def test_batch_writes_expected_layout_and_manifest(tiny_config):
    """Prove the Batch writes the correct file layout (Amazon, Walmart) and `run_manifest.json` matches the actual row and file counts on disk."""
    manifest = BatchRun(tiny_config).run()
    root = Path(tiny_config.storage.output_root)

    amazon_parts = sorted(
        root.glob("source-exports/channel=amazon/dataset=orders/export_date=*/part-sim*.jsonl")
    )
    walmart_parts = sorted(root.glob("pg-load/walmart.orders/export_date=*/part-sim*.jsonl"))
    assert amazon_parts and walmart_parts
    assert json.loads((root / "run_manifest.json").read_text()) == manifest

    # Row counts in the manifest match the actual rows in the files.
    amazon_rows = sum(len(p.read_text().splitlines()) for p in amazon_parts)
    assert manifest["datasets"]["amazon.orders"]["rows"] == amazon_rows
    assert manifest["datasets"]["amazon.orders"]["files"] == len(amazon_parts)


def test_amazon_partitions_before_switch_use_v0_and_after_use_new_schema(tiny_config):
    """Prove Amazon files before the switch day use the v0 key (`AmazonOrderId`), and from that day the 2026 key (`orderId`)."""
    BatchRun(tiny_config).run()
    root = Path(tiny_config.storage.output_root)
    switch_date = (
        tiny_config.day_start(tiny_config.offline_problems.amazon_orders_api_switch_day)
        .date()
        .isoformat()
    )
    for part in root.glob(
        "source-exports/channel=amazon/dataset=orders/export_date=*/part-sim*.jsonl"
    ):
        export_date = part.parent.name.removeprefix("export_date=")
        first = json.loads(part.read_text().splitlines()[0])
        expected_key = "AmazonOrderId" if export_date < switch_date else "orderId"
        assert expected_key in first


def test_no_labels_yet_when_history_is_shorter_than_window(tiny_config):
    """12 history days is shorter than the 30-day window, so no line has a known label yet.

    Days without labels create no Parquet file, so here the whole directory is absent.
    What says "that day is done" is the stamp, not the presence of a file.
    """
    BatchRun(tiny_config).run()
    assert not sorted(Path(tiny_config.storage.output_root).glob("ground-truth/labels/*.parquet"))


def test_labels_only_include_lines_known_before_cutoff(tiny_config):
    """Prove the label table only holds Order Lines with a known label before the Cutoff, labels are int8 and only 0/1."""
    scale = dataclasses.replace(tiny_config.scale, days_history=40, orders_per_day=20)
    config = dataclasses.replace(tiny_config, scale=scale)
    BatchRun(config).run()
    table = pq.read_table(Path(config.storage.output_root) / "ground-truth/labels")
    assert table.num_rows > 0
    # The int8 type comes from `LABELS_SCHEMA`; without a schema pyarrow infers types and
    # a day with no labels would become type `null`, making Spark read mismatched column types.
    assert str(table.schema.field("label").type) == "int8"
    assert set(table.column("label").to_pylist()) <= {0, 1}
    assert max(table.column("label_available_at").to_pylist()) <= config.cutoff


def test_same_config_produces_identical_files(tiny_config, tmp_path):
    """Prove the same config yields byte-identical files, even when written to a different directory."""
    other = dataclasses.replace(
        tiny_config,
        storage=dataclasses.replace(tiny_config.storage, output_root=str(tmp_path / "again")),
    )
    BatchRun(tiny_config).run()
    BatchRun(other).run()
    first = Path(tiny_config.storage.output_root)
    second = Path(other.storage.output_root)
    for part in first.glob("source-exports/**/*.jsonl"):
        assert part.read_bytes() == (second / part.relative_to(first)).read_bytes()


def test_seller_skus_cover_catalog_without_hidden_propensity(tiny_config):
    """Prove the SKU table covers the whole catalog and does not expose `defect_propensity` (the core's hidden value)."""
    manifest = BatchRun(tiny_config).run()
    part = Path(tiny_config.storage.output_root) / "pg-load/seller.skus/part-00000.jsonl"
    rows = [json.loads(line) for line in part.read_text().splitlines()]
    assert len(rows) == tiny_config.scale.n_skus == manifest["datasets"]["seller.skus"]["rows"]
    assert {"sku", "category", "base_price_usd"} <= set(rows[0])
    assert "defect_propensity" not in rows[0]


def test_unique_keys_equals_primary_placements_so_the_key_set_is_unneeded(tiny_config):
    """Pin the invariant that lets `DatasetStats` count unique keys without holding a key set.

    `BatchRun` no longer collects `rendered.key` into a set (at 100 GB scale that is ~117
    million strings in RAM) but takes `unique_keys` from the number of `primary` placements. This test
    rebuilds the real key set from the output files and compares, so if a Channel later
    generates keys that depend on the export day, or `plan_exports` yields more than one primary, the
    test fails instead of `duplicate_rate` in the manifest being silently wrong.
    """
    manifest = BatchRun(tiny_config).run()
    root = Path(tiny_config.storage.output_root)
    key_fields = {
        "amazon.orders": ("AmazonOrderId", "orderId"),
        "walmart.orders": ("purchase_order_id",),
    }

    for dataset, pattern in [
        (
            "amazon.orders",
            "source-exports/channel=amazon/dataset=orders/export_date=*/part-sim*.jsonl",
        ),
        ("walmart.orders", "pg-load/walmart.orders/export_date=*/part-sim*.jsonl"),
    ]:
        summary = manifest["datasets"][dataset]
        keys = {
            next(record[f] for f in key_fields[dataset] if f in record)
            for part in sorted(root.glob(pattern))
            for record in map(json.loads, part.read_text().splitlines())
        }
        assert len(keys) == summary["rows_by_placement"]["primary"], dataset
        assert len(keys) == summary["unique_keys"], dataset


def test_ground_truth_is_one_parquet_file_per_simulated_day_with_rows(tiny_config):
    """Labels and key mappings are written incrementally so RAM is per day, and each day closes its own file.

    One file for the whole run cannot be resumed: the Parquet footer is only written
    on close, so an interruption midway loses the entire file even with 90% of rows on disk.
    With one file per day, every closed day is readable right away.
    """
    BatchRun(tiny_config).run()
    root = Path(tiny_config.storage.output_root)

    parts = sorted(root.glob("ground-truth/order_line_keys/amazon/part-sim*.parquet"))
    assert 1 < len(parts) <= tiny_config.scale.days_history
    for part in parts:
        meta = pq.ParquetFile(part)
        assert meta.num_row_groups == 1, part.name
        assert meta.metadata.num_rows > 0, part.name


def test_manifest_bytes_match_the_real_size_of_the_export_files(tiny_config):
    """`datasets[*].bytes` is the measure for the 100 GB threshold, so it must equal the actual bytes on disk.

    The byte count now comes from `JsonlWriterPool.close()` when the file closes, no longer from one
    `write_jsonl` for the whole day. This test is why that total can be trusted as evidence
    of reaching the threshold instead of `du`.
    """
    manifest = BatchRun(tiny_config).run()
    root = Path(tiny_config.storage.output_root)
    patterns = {
        "amazon.orders": "source-exports/channel=amazon/dataset=orders/export_date=*/part-sim*.jsonl",
        "walmart.orders": "pg-load/walmart.orders/export_date=*/part-sim*.jsonl",
        "seller.skus": "pg-load/seller.skus/part-00000.jsonl",
    }
    for dataset, pattern in patterns.items():
        parts = sorted(root.glob(pattern))
        assert parts, dataset
        assert manifest["datasets"][dataset]["bytes"] == sum(p.stat().st_size for p in parts), (
            dataset
        )
        assert manifest["datasets"][dataset]["files"] == len(parts), dataset
