"""Test reading the Walmart v2 bootstrap file before loading PostgreSQL (no DB needed)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from generator.generate_order_lifecycle_fixture import generate_fixture

SCRIPT = Path(__file__).resolve().parents[4] / "scripts" / "setup/load_order_lifecycle_walmart.py"
spec = importlib.util.spec_from_file_location("scripts.setup.load_order_lifecycle_walmart", SCRIPT)
loader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(loader)


@pytest.fixture(scope="module")
def fixture_root(tmp_path_factory):
    """Generate a small v2 source fixture to check the Walmart bootstrap."""
    root = tmp_path_factory.mktemp("v2") / "fixture"
    generate_fixture(root)
    return root


def test_rows_cover_manifest_snapshot_scope(fixture_root):
    """Check the bootstrap rows cover exactly the snapshot scope in the manifest."""
    manifest = json.loads((fixture_root / "manifest.json").read_text())
    rows = loader.bootstrap_rows(fixture_root)
    assert [r.purchase_order_id for r in rows] == manifest["native_sources"]["walmart"][
        "snapshot_scope"
    ]["order_ids"]
    assert {r.dataset_version for r in rows} == {manifest["dataset_version"]}


def test_raw_payload_text_is_byte_identical_to_file(fixture_root):
    """Check the raw payload is byte-identical to the source file."""
    rows = loader.bootstrap_rows(fixture_root)
    path = next((fixture_root).rglob("feed=walmart.orders/*/part-00000.jsonl"))
    first_line = path.read_text().splitlines()[0]
    assert '"raw_payload":' + rows[0].raw_payload + "}" in first_line


def test_rejects_root_without_walmart_file(tmp_path):
    """Check a source missing the Walmart file is rejected."""
    (tmp_path / "manifest.json").write_text(
        json.dumps({"dataset_version": "dataset-x", "files": []})
    )
    with pytest.raises(ValueError, match="walmart.orders"):
        loader.bootstrap_rows(tmp_path)
