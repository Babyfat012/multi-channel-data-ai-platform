"""Source contract v2 exercised through frozen local exports, never truth labels."""

import hashlib
import json

import pytest

from generator.generate_order_lifecycle_fixture import FixtureConfig, generate_fixture


def read_rows(root, manifest, feed):
    """Read all exported rows belonging to a manifest feed."""
    return [
        json.loads(line)
        for item in manifest["files"]
        if item["feed"] == feed
        for line in (root / item["path"]).read_text().splitlines()
    ]


def test_new_local_fixture_pins_five_native_sources_and_operational_contract(tmp_path):
    """Verify frozen local exports cover five native sources and operational contracts."""
    root = tmp_path / "new-v2"
    m = generate_fixture(root)
    assert json.loads((root / "manifest.json").read_text()) == m
    assert m["contract_id"] == "seller-source-v2"
    assert m["configuration"]["creation_days"] == 3
    assert m["counts"]["orders"] == 15
    assert m["counts"]["skus"] == 2
    assert set(m["native_sources"]) == {"amazon", "walmart", "shopify", "tiktok_shop", "shopee"}
    assert m["native_sources"]["walmart"]["transport"] == "postgres_bootstrap_only"
    assert m["native_sources"]["walmart"]["ingestion_boundary"] == "jdbc"
    feeds = {item["feed"] for item in m["files"]}
    assert {
        "seller_oms.line_commitments",
        "seller_oms.fulfillment_allocations",
        "carrier.delivery_events",
        "seller_postsales.return_events",
        "seller_postsales.refund_events",
        "seller_catalog.listing_versions",
        "seller_wms.stock_operations",
        "seller_wms.inbound_events",
        "seller_wms.stock_snapshots",
        "source.coverage_envelopes",
        "source.uncertainties",
    } <= feeds
    for item in m["files"]:
        raw = (root / item["path"]).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == item["sha256"]
        assert len(raw) == item["bytes"]
    assert sum(item["bytes"] for item in m["files"]) < 5_000_000
    before = (root / "manifest.json").read_bytes()
    with pytest.raises(FileExistsError):
        generate_fixture(root)
    assert (root / "manifest.json").read_bytes() == before
    with pytest.raises(ValueError, match="local"):
        generate_fixture("s3://forbidden/new")


def test_quantity_money_cancellation_and_stock_balance_are_observable(tmp_path):
    """Verify quantity, money, cancellations and stock balances are observable."""
    m = generate_fixture(tmp_path / "v2")
    root = tmp_path / "v2"
    commitments = read_rows(root, m, "seller_oms.line_commitments")
    final = [r["payload"] for r in commitments if r["payload"]["fulfillment_terminal"]]
    first = next(p for p in final if p["native_line_id"] == "1")
    assert (first["ordered_basis_qty"], first["canceled_unshipped_qty"]) == (3, 1)
    assert first["ordered_product_net_minor"] - first["canceled_product_net_minor"] == 20000
    deliveries = read_rows(root, m, "carrier.delivery_events")
    for p in final:
        delivered = [
            unit
            for r in deliveries
            if r["payload"]["line_id"] == p["line_id"]
            for unit in r["payload"]["delivered_unit_ids"]
        ]
        assert len(delivered) == len(set(delivered)) == 3 - p["canceled_unshipped_qty"]
        assert not set(delivered) & set(p.get("canceled_unit_ids", []))
    # Independent chronological projection over exported deltas, not generator truth.
    balances = {}
    operations = read_rows(root, m, "seller_wms.stock_operations")
    seen = set()
    for r in sorted(operations, key=lambda r: (r["effective_at"], r["source_sequence"])):
        p = r["payload"]
        assert set(p["depends_on_operation_ids"]) <= seen
        seen.add(p["operation_id"])
        state = balances.setdefault(p["internal_sku_id"], [0, 0, 0])
        for i, field in enumerate(("delta_on_hand", "delta_reserved", "delta_non_sellable")):
            state[i] += p[field]
        assert min(state) >= 0
        assert state[0] - state[1] - state[2] >= 0
    assert balances == {"sku-A": [187, 0, 0], "sku-B": [157, 0, 0]}
    for r in read_rows(root, m, "seller_wms.stock_snapshots"):
        p = r["payload"]
        assert [p["on_hand"], p["reserved"], p["non_sellable"]] == balances[p["internal_sku_id"]]
    refunds = read_rows(root, m, "seller_postsales.refund_events")
    assert {r["payload"]["component"] for r in refunds} >= {"product", "shipping"}
    # No feature/label oracle leaks or binary float money in source payloads.
    for item in m["files"]:
        for line in (root / item["path"]).read_text().splitlines():

            def check(v):
                """Walk the payload recursively, rejecting floats and label-leaking fields."""
                assert not isinstance(v, float)
                if isinstance(v, dict):
                    assert not {"label", "return_probability", "defect_propensity"} & v.keys()
                    for x in v.values():
                        check(x)
                if isinstance(v, list):
                    for x in v:
                        check(x)

            check(json.loads(line))


def test_renderer_variants_keep_physical_world_and_unit_outcomes(tmp_path):
    """Verify renderer variants preserve the physical world and unit outcomes."""
    from generator.generate_order_lifecycle_fixture import require_same_world

    a = generate_fixture(tmp_path / "a")
    b = generate_fixture(tmp_path / "b", delivery_parts=2, native_schema=2)
    require_same_world(a, b)
    assert a["dataset_version"] != b["dataset_version"]
    assert a["physical_outcome_sha256"] == b["physical_outcome_sha256"]

    def outcomes(root, m):
        """Get sorted (unit, actual delivery time) pairs from the delivery feed."""
        return sorted(
            (u, r["payload"]["actual_delivered_at"])
            for r in read_rows(root, m, "carrier.delivery_events")
            for u in r["payload"]["delivered_unit_ids"]
        )

    assert outcomes(tmp_path / "a", a) == outcomes(tmp_path / "b", b)
    assert len(read_rows(tmp_path / "b", b, "carrier.delivery_events")) > len(
        read_rows(tmp_path / "a", a, "carrier.delivery_events")
    )
    amazon = read_rows(tmp_path / "b", b, "amazon.orders")[0]
    assert "orderId" in amazon and "AmazonOrderId" not in amazon
    with pytest.raises(ValueError, match="physical"):
        require_same_world(a, {"configuration": {"seed": 7}})
    c = generate_fixture(tmp_path / "c", FixtureConfig(seed=8))
    with pytest.raises(ValueError, match="physical"):
        require_same_world(a, c)


def test_uncertainty_resolution_is_a_later_source_revision_and_coverage_is_as_known(tmp_path):
    """Verify uncertainty resolves through a later revision with as-known coverage."""
    root = tmp_path / "v2"
    m = generate_fixture(root)
    uncertainty = read_rows(root, m, "source.uncertainties")
    assert {r["resolution_state"] for r in uncertainty} == {"unresolved", "resolved"}
    refunds = read_rows(root, m, "seller_postsales.refund_events")
    ambiguous = next(r for r in refunds if r["payload"].get("unresolved_scope_ref"))
    u = next(
        r
        for r in uncertainty
        if r["uncertainty_id"] == ambiguous["payload"]["unresolved_scope_ref"]
    )
    assert u["affected_scope"]["candidate_native_line_ids"] == ["1", "2"]
    assert ambiguous["payload"]["allocation_money_rows"] == []
    corrected = next(
        r for r in refunds if r["corrects_source_event_id"] == ambiguous["source_event_id"]
    )
    assert corrected["operation"] == "correction"
    assert corrected["entity_revision"] == ambiguous["entity_revision"] + 1
    assert corrected["effective_at"] == ambiguous["effective_at"]
    assert corrected["source_observed_at"] > ambiguous["source_observed_at"]
    assert corrected["payload"]["allocation_money_rows"][0]["amount_minor"] == 10000
    envelopes = [
        r
        for r in read_rows(root, m, "source.coverage_envelopes")
        if r["feed"] == "seller_postsales.refund_events" and r["status"] == "complete"
    ]
    assert [r["revision"] for r in envelopes] == [1, 2]
    assert u["uncertainty_id"] in envelopes[0]["unresolved_scope_refs"]
    assert envelopes[1]["unresolved_scope_refs"] == []
    assert (
        envelopes[0]["source_observed_at"]
        < corrected["source_observed_at"]
        < envelopes[1]["source_observed_at"]
    )
    for cov in envelopes:
        known = [r for r in refunds if r["source_observed_at"] < cov["source_observed_at"]]
        raw = json.dumps(
            sorted(known, key=lambda r: r["source_event_id"]),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
        assert hashlib.sha256(raw).hexdigest() == cov["record_set_sha256"]
        assert len(known) == cov["source_record_count"]


def test_horizons_are_exclusive_and_short_tail_is_rejected_before_writing(tmp_path):
    """Verify exclusive horizons and reject insufficient tails before writing."""
    root = tmp_path / "v2"
    m = generate_fixture(root)
    h = m["horizons"]
    assert h["order_creation_end"] == "2026-01-04T00:00:00.000000Z"
    assert h["lifecycle_followup_end"] == "2026-04-01T00:00:00.000000Z"
    assert h["source_observation_end"] == "2026-04-16T00:00:00.000000Z"
    assert m["tail_diagnostics"]["right_censored_lines"] == 0
    assert m["tail_diagnostics"]["terminal_lines"] == 30
    assert m["tail_diagnostics"]["bounds"]["max_source_delay_seconds"] <= 15 * 86400
    deliveries = read_rows(root, m, "carrier.delivery_events")
    assert max(r["effective_at"] for r in deliveries) > h["order_creation_end"]
    for feed in (
        "seller_oms.line_commitments",
        "carrier.delivery_events",
        "seller_postsales.return_events",
    ):
        for r in read_rows(root, m, feed):
            assert h["order_creation_start"] <= r["effective_at"] < h["lifecycle_followup_end"]
            assert r["effective_at"] <= r["source_observed_at"] < h["source_observation_end"]
    for bad in (
        FixtureConfig(followup_days=20, observation_days=35),
        FixtureConfig(observation_days=92),
        FixtureConfig(creation_days=365),
    ):
        target = tmp_path / "bad"
        with pytest.raises(ValueError):
            generate_fixture(target, bad)
        assert not target.exists()


def test_listing_business_change_and_correction_preserve_sale_and_stock_bindings(tmp_path):
    """Verify listing changes and corrections preserve sale and stock bindings."""
    root = tmp_path / "v2"
    m = generate_fixture(root)
    listing_rows = read_rows(root, m, "seller_catalog.listing_versions")
    changes = [r for r in listing_rows if r["entity_revision"] == 2]
    corrections = [r for r in listing_rows if r["operation"] == "correction"]
    assert len(changes) == len(corrections) == 1
    change, correction = changes[0], corrections[0]
    assert change["effective_at"] < change["source_observed_at"]
    assert correction["corrects_source_event_id"] == change["source_event_id"]
    assert change["payload"]["eligible_pool_ids"] == ["seller-secondary-1"]
    assert correction["payload"]["internal_sku_id"] == "sku-B"
    first = next(
        r
        for r in listing_rows
        if r["entity_id"] == change["entity_id"] and r["entity_revision"] == 1
    )
    assert first["payload"]["internal_sku_id"] == "sku-A"
    commitments = read_rows(root, m, "seller_oms.line_commitments")
    affected = [
        r["payload"]
        for r in commitments
        if r["payload"]["channel"] == "amazon" and r["payload"]["native_line_id"] == "1"
    ]
    assert len({p["line_id"] for p in affected}) == 3
    assert {p["internal_sku_id"] for p in affected} == {"sku-A"}
    assert {p["sale_mapping_version"] for p in affected} == {first["payload"]["mapping_version"]}
    allocations = read_rows(root, m, "seller_oms.fulfillment_allocations")
    assert {r["payload"]["bound_pool_id"] for r in allocations} == {"seller-shared-1"}
    openings = [
        r
        for r in read_rows(root, m, "seller_wms.stock_operations")
        if r["payload"]["operation_type"] == "opening"
    ]
    assert len(openings) == 2  # Seller total is 200 per SKU, never 5 storefronts * 200.
    assert {r["payload"]["quantity"] for r in openings} == {200}


def test_coverage_distinguishes_transport_gaps_from_unsupported_capability(tmp_path):
    """Verify coverage separates transport gaps from unsupported capabilities."""
    from generator.generate_order_lifecycle_fixture import coverage_receipt

    root = tmp_path / "v2"
    m = generate_fixture(root)
    rows = read_rows(root, m, "carrier.delivery_events")
    cov = next(
        c
        for c in read_rows(root, m, "source.coverage_envelopes")
        if c["feed"] == "carrier.delivery_events"
    )
    assert coverage_receipt(rows, cov)["status"] == "complete"
    missing = coverage_receipt(rows[:-1], cov)
    assert missing["status"] == "partial"
    assert "missing_sequence" in missing["reasons"]
    assert coverage_receipt(rows, {**cov, "completed_partitions": []})["status"] == "partial"
    assert (
        coverage_receipt(rows, {**cov, "expected_partitions": ["partition-0", "partition-1"]})[
            "status"
        ]
        == "partial"
    )
    unavailable = coverage_receipt([], {**cov, "capability_status": "unavailable"})
    assert unavailable["status"] == "unavailable"
    assert unavailable["reasons"] == ["missing_capability"]
    bad = json.loads(json.dumps(rows))
    bad[0]["payload"]["delivered_qty"] = 99
    assert "record_set_digest_mismatch" in coverage_receipt(bad, cov)["reasons"]
    assert coverage_receipt(rows + [rows[0]], cov)["status"] == "partial"
    assert "duplicate_source_identity" in coverage_receipt(rows + [rows[0]], cov)["reasons"]
    assert any(
        a["status"] == "unavailable" and a["scope"]["pool_ids"] == ["channel-managed-1"]
        for a in m["authority_capabilities"]
    )


def test_frozen_identity_probes_keep_retries_revisions_and_causal_pending_distinct(tmp_path):
    """Verify identities distinguish retries, revisions and pending causal dependencies."""
    from generator.generate_order_lifecycle_fixture import inspect_records, stable_id

    root = tmp_path / "v2"
    m = generate_fixture(root)
    probes = {p["case"]: p for p in read_rows(root, m, "fixture.probes")}
    assert set(probes) >= {
        "retry",
        "hash_conflict",
        "release_before_reserve",
        "missing_dependency",
        "revision_gap",
    }
    assert inspect_records(probes["retry"]["records"])["duplicates"] == 1
    assert inspect_records(probes["hash_conflict"]["records"])["status"] == "conflict"
    assert inspect_records(probes["missing_dependency"]["records"])["status"] == "pending_reference"
    assert inspect_records(probes["revision_gap"]["records"])["status"] == "pending_revision"
    assert inspect_records(probes["release_before_reserve"]["records"])["status"] == "ready"
    assert (
        inspect_records(list(reversed(probes["release_before_reserve"]["records"])))["status"]
        == "ready"
    )
    all_rows = [
        r
        for feed in (
            "seller_oms.line_commitments",
            "seller_oms.fulfillment_allocations",
            "carrier.delivery_events",
            "seller_postsales.return_events",
            "seller_postsales.refund_events",
            "seller_wms.stock_operations",
            "seller_wms.inbound_events",
            "seller_catalog.listing_versions",
            "seller_wms.stock_snapshots",
        )
        for r in read_rows(root, m, feed)
    ]
    assert inspect_records(all_rows)["status"] == "ready"
    for producer in {r["producer"] for r in all_rows}:
        ordered = sorted(
            [r for r in all_rows if r["producer"] == producer], key=lambda r: r["source_sequence"]
        )
        assert [r["source_sequence"] for r in ordered] == list(range(1, len(ordered) + 1))
        assert [r["source_observed_at"] for r in ordered] == sorted(
            r["source_observed_at"] for r in ordered
        )
    by_id = {r["source_event_id"]: r for r in all_rows}
    for r in all_rows:
        for ref in r["depends_on_event_ids"]:
            assert by_id[ref]["source_observed_at"] <= r["source_observed_at"]
    assert stable_id("line", "a|b", "c") != stable_id("line", "a", "b|c")
    assert stable_id("line", {"id": None}) != stable_id("line", {})
    assert stable_id("line", "account-1", "1") != stable_id("line", "account-2", "1")


def test_partial_shipments_have_distinct_allocations_and_terminal_time(tmp_path):
    """Verify partial shipments have separate allocations and terminal timestamps."""
    root = tmp_path / "v2"
    m = generate_fixture(root)
    events = read_rows(root, m, "carrier.delivery_events")
    line = next(r["payload"]["line_id"] for r in events if r["payload"]["native_line_id"] == "2")
    delivered = [r for r in events if r["payload"]["line_id"] == line]
    assert len({r["payload"]["allocation_id"] for r in delivered}) == 2
    assert sorted(r["payload"]["delivered_qty"] for r in delivered) == [1, 2]
    assert len({r["effective_at"] for r in delivered}) == 2
    commitments = [
        r
        for r in read_rows(root, m, "seller_oms.line_commitments")
        if r["payload"]["line_id"] == line
    ]
    terminal = next(r for r in commitments if r["payload"]["fulfillment_terminal"])
    assert terminal["effective_at"] == max(r["effective_at"] for r in delivered)
    assert min(r["effective_at"] for r in delivered) < terminal["effective_at"]


def test_approval_timing_scenario_changes_world_but_keeps_affected_intents(tmp_path):
    """Verify approval timing changes the world while retaining affected intents."""
    from datetime import datetime

    from generator.generate_order_lifecycle_fixture import require_same_world

    a = generate_fixture(tmp_path / "a")
    b = generate_fixture(tmp_path / "b", FixtureConfig(approval_lag_days=31))
    for root, manifest, days in ((tmp_path / "a", a, 29), (tmp_path / "b", b, 31)):
        deliveries = read_rows(root, manifest, "carrier.delivery_events")
        returns = read_rows(root, manifest, "seller_postsales.return_events")
        approved = [r for r in returns if r["payload"]["event_type"] == "approved"]
        assert len(approved) == 15
        for r in approved:
            delivered = next(
                d
                for d in deliveries
                if d["payload"]["allocation_id"] == r["payload"]["allocation_id"]
            )
            assert (
                datetime.fromisoformat(r["effective_at"])
                - datetime.fromisoformat(delivered["effective_at"])
            ).days == days
    # Intent digest excludes timing policy; physical outcome digest includes it.
    assert a["intent_sha256"] == b["intent_sha256"]
    with pytest.raises(ValueError, match="physical"):
        require_same_world(a, b)


def test_pinned_profiles_and_recreated_sources_have_stable_fact_identity(tmp_path):
    """Verify pinned profiles and regenerated exports preserve fact identity."""
    from generator.generate_order_lifecycle_fixture import inspect_records

    a = generate_fixture(tmp_path / "a")
    b = generate_fixture(tmp_path / "b")
    assert a["dataset_version"] == b["dataset_version"]
    pinned = json.loads((tmp_path / "a" / "identity.json").read_text())
    assert pinned["dataset_version"] == a["dataset_version"]
    # The fixture freezes the baseline contract; the current docs have weekly coverage only for scale.
    # Compare the historical hash, so the fixture identity/bytes do not change as the contract doc evolves.
    assert (
        pinned["identity"]["contract_sha256"]
        == "84fa3bb73775e418e8e7ea5fc69be20f4f9324e99abc7f43a9aee0af7faacabf"
    )
    assert pinned["identity"]["source_profiles"]["seller-operated-label-v2/1"][
        "required_feeds"
    ] == [
        "seller_oms.line_commitments",
        "seller_oms.fulfillment_allocations",
        "carrier.delivery_events",
        "seller_postsales.return_events",
        "seller_postsales.refund_events",
        "source.coverage_envelopes",
        "source.uncertainties",
    ]
    left = read_rows(tmp_path / "a", a, "carrier.delivery_events")
    right = read_rows(tmp_path / "b", b, "carrier.delivery_events")
    result = inspect_records(left + right)
    assert result["conflict_event_ids"] == []
    assert result["duplicates"] == len(left)


def test_native_quantity_and_absence_probes_keep_commitment_and_stock_correction_refs(tmp_path):
    """Verify native quantity and absence probes retain commitment and correction references."""
    root = tmp_path / "v2"
    m = generate_fixture(root)
    probes = {p["case"]: p for p in read_rows(root, m, "fixture.probes")}
    native = probes["native_quantity_then_absence"]
    assert native["records"][0]["order"]["line_items"][0]["current_quantity"] == 2
    assert native["records"][1]["order"]["line_items"] == []
    assert native["snapshot_manifest"]["snapshot_mode"] == "full_entity"
    assert native["snapshot_manifest"]["page_set_complete"] is True
    original = next(
        r
        for r in read_rows(root, m, "seller_oms.line_commitments")
        if r["payload"]["line_id"] == native["line_ref"]
    )
    assert original["payload"]["ordered_basis_qty"] == 3
    stock = probes["stock_correction_reordered"]["records"]
    assert stock[0]["operation"] == "correction"
    assert stock[0]["corrects_source_event_id"] == stock[1]["source_event_id"]
    assert stock[0]["payload"]["operation_id"] == stock[1]["payload"]["operation_id"]
    assert stock[0]["payload"]["delta_on_hand"] == 199
    assert all(
        item["role"] == "diagnostic_fixture"
        for item in m["files"]
        if item["feed"] == "fixture.probes"
    )
