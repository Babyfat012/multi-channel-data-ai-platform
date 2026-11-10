"""HTML rendering part of render_model_lineage (no Chrome needed)."""

from scripts.evidence.render_model_lineage import page, started, verdict


def receipt(match=True):
    """Minimal receipt with the same structure as `mlflow_run_data_proof.py`."""
    art = {"path": "data/train/train_set", "bytes": 1234, "sha256": "ab" * 32}
    return {
        "model": {"name": "return-risk", "version": "1", "source": "models:/m-1", "run_id": "r1"},
        "run_name": "run-x",
        "run_id": "r1",
        "started": 1793249613108,
        "params": {"algo": "lightgbm", "seed": "7"},
        "metrics": {"pr_auc_model": 0.252, "pr_auc_baseline": 0.247, "test_rows": 16411.0},
        "tags": {
            "data.dataset_version": "d",
            "data.generation_id": "g",
            "data.entity_path": "s3://x",
        },
        "train": {"tag": "ab" * 32, "artifacts": [art], "sha256_match": match},
        "test": {"tag": "cd" * 32, "artifacts": [], "sha256_match": True},
    }


def test_started_is_utc_and_verdict_flags_mismatch():
    """Timestamp is converted to UTC; a SHA mismatch must show ✗, never silently report a match."""
    assert started(1793249613108) == "2026-10-29 04:53 UTC"
    assert "✓" in verdict(True) and "✗" in verdict(False)


def test_page_shows_chain_and_hash_verdict():
    """The image has all four blocks and correctly reflects the hash comparison result."""
    ok, bad = page(receipt(True)), page(receipt(False))
    for text in ("return-risk", "run-x", "lightgbm", "1,234 byte", "ab" * 32, "16,411"):
        assert text in ok
    assert "✓ match" in ok and "✗ MISMATCH" not in ok
    assert "✗ MISMATCH" in bad
