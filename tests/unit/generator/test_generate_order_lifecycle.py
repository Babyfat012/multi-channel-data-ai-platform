"""Check the scale contract, reproducibility and default fixture invariants."""

import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from pathlib import Path

import pytest

from generator.generate_order_lifecycle import SCENARIO_WEIGHTS, ScaleConfig, generate_scaled
from generator.generate_order_lifecycle_fixture import (
    FEEDS,
    PROFILES,
    coverage_receipt,
    generate_fixture,
    inspect_records,
)


def test_fixture_default_bytes_are_pinned(tmp_path, monkeypatch):
    """Fix the execution clock, then compare every digest saved before the refactor."""
    import generator.generate_order_lifecycle_fixture as source

    class Fixed(datetime):
        """Fixed execution clock of the baseline."""

        @classmethod
        def now(cls, tz=None):
            """Return the baseline UTC time."""
            return cls(2026, 10, 19, tzinfo=timezone.utc)

    monkeypatch.setattr(source, "datetime", Fixed)
    expected = json.loads(
        (Path(__file__).parent / "order_lifecycle_fixture_digests.json").read_text()
    )
    manifest = generate_fixture(tmp_path / "fixture")
    assert manifest["physical_outcome_sha256"] == expected["physical_outcome_sha256"]
    assert manifest["dataset_version"] == expected["dataset_version"]
    assert {f["path"]: f["sha256"] for f in manifest["files"]} == expected["files"]
    for item in manifest["files"]:
        assert (
            hashlib.sha256((tmp_path / "fixture" / item["path"]).read_bytes()).hexdigest()
            == item["sha256"]
        )


def test_seed_recreates_sources(tmp_path):
    """The same seed keeps every file identical; a different seed changes the physical world."""
    from generator.generate_order_lifecycle import ScaleConfig, generate_scaled

    a = generate_scaled(tmp_path / "a", ScaleConfig(days=2, orders_per_day=20))
    b = generate_scaled(tmp_path / "b", ScaleConfig(days=2, orders_per_day=20))
    c = generate_scaled(tmp_path / "c", ScaleConfig(days=2, orders_per_day=20, seed=8))
    assert a["physical_outcome_sha256"] == b["physical_outcome_sha256"]
    assert [(f["path"], f["sha256"]) for f in a["files"]] == [
        (f["path"], f["sha256"]) for f in b["files"]
    ]
    assert a["physical_outcome_sha256"] != c["physical_outcome_sha256"]


def rows_for(root, manifest, feed):
    """Read exactly the feeds listed in the source manifest."""
    return [
        json.loads(row)
        for item in manifest["files"]
        if item["feed"] == feed
        for row in (root / item["path"]).read_text().splitlines()
    ]


@pytest.fixture(scope="module")
def sample(tmp_path_factory):
    """A sample of exactly 5,000 lines for ratio checks and the independent oracle."""
    root = tmp_path_factory.mktemp("scale-sample") / "data"
    config = ScaleConfig(days=1, orders_per_day=5000, lines_per_order=(1.0, 0.0, 0.0, 0.0))
    manifest = generate_scaled(root, config)
    truth = [json.loads(row) for row in (root / "_truth/lines.jsonl").read_text().splitlines()]
    return root, manifest, truth


def test_sampled_rates_and_sku_skew(sample):
    """The 5,000-line sample has every scenario within ±3 percentage points, hot SKU >10%."""
    _, manifest, truth = sample
    assert len(truth) == 5000
    counts = Counter(row["scenario"] for row in truth)
    for scenario, probability in SCENARIO_WEIGHTS.items():
        assert abs(counts[scenario] / len(truth) - probability) <= 0.03
    assert counts == manifest["scenario_distribution"]["counts"]
    skus = Counter(row["sku"] for row in truth)
    assert skus.most_common(1)[0][1] / len(truth) > 0.10


def test_uniform_skus_have_no_hot_rank(tmp_path):
    """s=0 gives a top SKU below twice the mean at the same sample size."""
    root = tmp_path / "uniform"
    manifest = generate_scaled(
        root,
        ScaleConfig(
            days=1, orders_per_day=5000, sku_zipf_s=0, lines_per_order=(1.0, 0.0, 0.0, 0.0)
        ),
    )
    skus = Counter(
        json.loads(r)["sku"] for r in (root / "_truth/lines.jsonl").read_text().splitlines()
    )
    assert max(skus.values()) < 2 * manifest["counts"]["lines"] / 100


def test_source_contract_identities_coverage_and_profiles(sample):
    """Every source passes inspect/gate; coverage matches receipts, all feeds of the three profiles present."""
    from spark_jobs.lib.data_contracts.gate import gate_root
    from spark_jobs.lib.data_contracts.registry import Registry

    root, manifest, _ = sample
    feeds = {item["feed"] for item in manifest["files"]}
    for profile in (
        "core-orders-five-channel/1",
        "seller-operated-label-v2/1",
        "seller-stock-triage/1",
    ):
        assert set(PROFILES[profile]["required_feeds"]) <= feeds
    rows = [row for feed in FEEDS for row in rows_for(root, manifest, feed)]
    inspected = inspect_records(rows)
    assert inspected["status"] == "ready"
    assert inspected["duplicates"] == 0
    assert (
        inspected["conflict_event_ids"]
        == inspected["pending_event_ids"]
        == inspected["revision_gap_event_ids"]
        == []
    )
    assert gate_root(root, Registry("contracts"))["quarantine"]["total"] == 0
    for cov in rows_for(root, manifest, "source.coverage_envelopes"):
        matching = [
            r
            for r in rows
            if r["feed"] == cov["feed"]
            and r["source_observed_at"] < cov["source_observed_at"]
            and cov["event_interval"]["start_inclusive"]
            <= r["effective_at"]
            < cov["event_interval"]["end_exclusive"]
        ]
        assert coverage_receipt(matching, cov)["status"] == (
            "complete" if cov["status"] == "complete" else "unavailable"
        )
    by_id = {r["source_event_id"]: r for r in rows}
    for row in rows:
        assert (
            manifest["horizons"]["order_creation_start"]
            <= row["effective_at"]
            < manifest["horizons"]["lifecycle_followup_end"]
        )
        assert (
            row["effective_at"]
            <= row["source_observed_at"]
            < manifest["horizons"]["source_observation_end"]
        )
        assert all(
            by_id[ref]["source_observed_at"] <= row["source_observed_at"]
            for ref in row["depends_on_event_ids"]
        )
    for producer in {r["producer"] for r in rows}:
        ordered = sorted(
            (r for r in rows if r["producer"] == producer), key=lambda r: r["source_sequence"]
        )
        assert [r["source_sequence"] for r in ordered] == list(range(1, len(ordered) + 1))
        assert [r["source_observed_at"] for r in ordered] == sorted(
            r["source_observed_at"] for r in ordered
        )
    for item in manifest["files"]:
        raw = (root / item["path"]).read_bytes()
        assert len(raw) == item["bytes"]
        assert hashlib.sha256(raw).hexdigest() == item["sha256"]
    assert not any(".scale-spool-" in str(p) for p in root.iterdir())


def test_truth_matches_physical_events_and_window_boundaries(sample):
    """The oracle checks quantities, times, retry/correction and censoring against the actually emitted sources."""
    root, manifest, truth = sample
    deliveries = rows_for(root, manifest, "carrier.delivery_events")
    returns = rows_for(root, manifest, "seller_postsales.return_events")
    refunds = rows_for(root, manifest, "seller_postsales.refund_events")
    commitments = rows_for(root, manifest, "seller_oms.line_commitments")
    per_line = defaultdict(
        lambda: {"delivered": [], "returns": [], "refunds": [], "commitments": []}
    )
    for kind, records in (
        ("delivered", deliveries),
        ("returns", returns),
        ("refunds", refunds),
        ("commitments", commitments),
    ):
        for row in records:
            per_line[row["payload"]["line_id"]][kind].append(row)
    assert len({r["payload"]["line_id"] for r in commitments}) == len(truth)
    for expected in truth:
        records = per_line[expected["line_id"]]
        assert (
            sum(r["payload"]["delivered_qty"] for r in records["delivered"])
            == expected["delivered"]
        )
        latest = max(records["commitments"], key=lambda r: r["entity_revision"])["payload"]
        assert latest["canceled_unshipped_qty"] == expected["canceled"]
        assert latest["fulfillment_terminal"] == (expected["scenario"] != "right_censored")
        approved = [r for r in records["returns"] if r["payload"]["event_type"] == "approved"]
        assert [r["effective_at"] for r in approved] == (
            [expected["approved_at"]] if expected["approved_at"] else []
        )
        successes = [
            r
            for r in records["refunds"]
            if r["payload"]["status"] == "success" and r["payload"]["component"] == "product"
        ]
        assert {r["effective_at"] for r in successes} == (
            {expected["refund_success_at"]} if expected["refund_success_at"] else set()
        )
        if expected["scenario"] in ("return_with_refund", "return_only", "return_after_window"):
            delta = datetime.fromisoformat(approved[0]["effective_at"]) - datetime.fromisoformat(
                records["delivered"][0]["effective_at"]
            )
            assert delta.days == (31 if expected["scenario"] == "return_after_window" else 29)
        if expected["scenario"] == "refund_retry":
            assert {r["payload"]["status"] for r in records["refunds"]} == {"failed", "success"}
        if expected["scenario"] == "correction":
            assert any(
                r["operation"] == "correction" and r["entity_revision"] == 2
                for r in records["refunds"]
            )
    balances = defaultdict(lambda: [0, 0, 0])
    for row in rows_for(root, manifest, "seller_wms.stock_operations"):
        p = row["payload"]
        for i, field in enumerate(("delta_on_hand", "delta_reserved", "delta_non_sellable")):
            balances[p["internal_sku_id"]][i] += p[field]
    for row in rows_for(root, manifest, "seller_wms.stock_snapshots"):
        p = row["payload"]
        assert [p["on_hand"], p["reserved"], p["non_sellable"]] == balances[p["internal_sku_id"]]
    assert manifest["truth"]["not_a_source"] is True
    assert all(not f["path"].startswith("_truth/") for f in manifest["files"])


@pytest.mark.parametrize(
    "config",
    [
        ScaleConfig(days=181),
        ScaleConfig(days=0),
        ScaleConfig(orders_per_day=20001),
        ScaleConfig(sku_count=1),
        ScaleConfig(sku_count=5001),
        ScaleConfig(sku_zipf_s=-1),
        ScaleConfig(sku_zipf_s=float("nan")),
        ScaleConfig(days=180, orders_per_day=20000),
        ScaleConfig(channel_weights=(0.0,) * 5),
        ScaleConfig(lines_per_order=(-1.0, 1.0, 1.0, 1.0)),
        ScaleConfig(start="2026-01-01T00:00:00+07:00"),
        ScaleConfig(start="2026-01-01T00:00:00"),
        ScaleConfig(followup_days=30),
        ScaleConfig(observation_days=271),
        ScaleConfig(native_schemas=(("walmart", 2),)),
    ],
)
def test_validation_rejects_before_writing(tmp_path, config):
    """Reject invalid or over-ceiling parameters before any filesystem effect."""
    with pytest.raises(ValueError):
        config.validate()
    with pytest.raises(ValueError):
        generate_scaled(tmp_path / "bad", config)
    assert not (tmp_path / "bad").exists()


def test_configuration_is_frozen_and_horizon_supports_180_days():
    """Immutable config, supporting the full 180-day domain when the row count is valid."""
    config = ScaleConfig(days=180, orders_per_day=1)
    config.validate()
    with pytest.raises(FrozenInstanceError):
        config.seed = 10


@pytest.mark.parametrize(
    "config",
    [
        ScaleConfig(days=2, orders_per_day=200),
        ScaleConfig(days=1, orders_per_day=500, sku_zipf_s=0, sku_count=50),
        ScaleConfig(days=1, orders_per_day=1, sku_count=5000),
    ],
)
def test_estimate_within_twenty_percent(tmp_path, config):
    """The pre-generation estimate is within 20% on source rows and bytes."""
    estimated = config.estimate()
    actual = generate_scaled(tmp_path / "estimated", config)
    records = sum(f["rows"] for f in actual["files"])
    assert abs(estimated["source_records"] / records - 1) < 0.20
    assert abs(estimated["source_bytes"] / actual["source_bytes"] - 1) < 0.20
    assert records <= estimated["source_record_upper_bound"]


def test_spark_jobs_never_read_truth():
    """No truth references in the Data Platform job tree."""
    import re

    for path in Path("spark_jobs").rglob("*"):
        if path.is_file() and "__pycache__" not in path.parts:
            assert not re.search(rb"(?<![A-Za-z0-9_])_truth(?:[/\\]|[\'\"])", path.read_bytes()), (
                str(path)
            )


def test_native_schema_overrides_and_existing_root(tmp_path):
    """Same physical world across renderer overrides; existing directories are protected."""
    config = ScaleConfig(days=2, orders_per_day=50, lines_per_order=(0.0, 0.0, 0.0, 1.0))
    plain = generate_scaled(tmp_path / "plain", config)
    from dataclasses import replace

    changed = generate_scaled(
        tmp_path / "changed",
        replace(config, native_schemas=(("amazon", 2), ("tiktok_shop", 2), ("shopee", 2))),
    )
    assert plain["physical_outcome_sha256"] == changed["physical_outcome_sha256"]
    assert plain["dataset_version"] != changed["dataset_version"]
    assert changed["native_sources"]["shopee"]["schema_id"] == "sim-native-v2/shopee/2"
    assert "orderId" in rows_for(tmp_path / "changed", changed, "amazon.orders")[0]
    assert (
        "order_number"
        in rows_for(tmp_path / "changed", changed, "shopee.orders")[0]["response"]["order_list"][0]
    )
    assert plain["counts"]["lines"] == 400
    before = (tmp_path / "plain/manifest.json").read_bytes()
    with pytest.raises(FileExistsError):
        generate_scaled(tmp_path / "plain", config)
    assert (tmp_path / "plain/manifest.json").read_bytes() == before
    with pytest.raises(ValueError, match="local"):
        generate_scaled("s3://forbidden", config)


def test_very_large_zipf_exponent_is_valid(tmp_path):
    """Every finite non-negative s is valid, even close to a single-SKU distribution."""
    config = ScaleConfig(days=1, orders_per_day=2, sku_count=10, sku_zipf_s=1000.0)
    config.validate()
    manifest = generate_scaled(tmp_path / "sharp", config)
    assert manifest["counts"]["orders"] == 2


def test_full_180_day_horizon_with_sparse_channels(tmp_path):
    """Full 180-day domain, few SKUs, empty Channels, and the inventory tail still balances."""
    root = tmp_path / "long-tail"
    manifest = generate_scaled(
        root,
        ScaleConfig(
            days=180, orders_per_day=1, sku_count=2, channel_weights=(1.0, 0.0, 0.0, 0.0, 0.0)
        ),
    )
    assert manifest["counts"]["orders"] == 180
    rows = [r for feed in FEEDS for r in rows_for(root, manifest, feed)]
    assert inspect_records(rows)["status"] == "ready"
    assert all(r["effective_at"] < manifest["horizons"]["lifecycle_followup_end"] for r in rows)
    assert rows_for(root, manifest, "walmart.orders") == []
    assert len(manifest["tail_diagnostics"]["cohorts"]) == 180
    balances = defaultdict(lambda: [0, 0, 0])
    for row in rows_for(root, manifest, "seller_wms.stock_operations"):
        p = row["payload"]
        for i, field in enumerate(("delta_on_hand", "delta_reserved", "delta_non_sellable")):
            balances[p["internal_sku_id"]][i] += p[field]
    for row in rows_for(root, manifest, "seller_wms.stock_snapshots"):
        p = row["payload"]
        assert [p["on_hand"], p["reserved"], p["non_sellable"]] == balances[p["internal_sku_id"]]


def test_sku_return_skew_creates_learnable_signal():
    """skew > 0: positive scenario rates differ clearly across SKUs and by Channel factor; with skew = 0 they do not."""
    from collections import Counter

    from generator.generate_order_lifecycle import (
        POSITIVE_SCENARIOS,
        ScaleConfig,
        _orders_by_day,
    )

    def rates(skew, key):
        config = ScaleConfig(
            days=30, orders_per_day=200, sku_count=5, sku_zipf_s=0.0, sku_return_skew=skew
        )
        pos, total = Counter(), Counter()
        for orders in _orders_by_day(config):
            for order in orders:
                for line in order["lines"]:
                    k = key(order, line)
                    total[k] += 1
                    pos[k] += line["scenario"] in POSITIVE_SCENARIOS
        return {k: pos[k] / total[k] for k in total}

    by_sku = rates(1.5, lambda o, line: line["sku"])
    flat = rates(0.0, lambda o, line: line["sku"])
    assert max(by_sku.values()) - min(by_sku.values()) > 0.2
    assert max(flat.values()) - min(flat.values()) < 0.06
    by_channel = rates(1.5, lambda o, line: o["channel"])
    assert by_channel["tiktok_shop"] > by_channel["walmart"]
