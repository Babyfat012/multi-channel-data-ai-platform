"""Independent hand-built oracles from return-label-contract.md; no simulator truth."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from spark_jobs.lib.return_label import label_line, sku_return_feature

D0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def day(n):
    """UTC instant, preserving fractional days for boundary tests."""
    return D0 + timedelta(days=n)


def event(kind, eid, n=0, observed=None, **payload):
    """Source-shaped observation with independently chosen event and known clocks."""
    return {
        **dict(
            kind=kind,
            source_event_id=eid,
            entity_id=eid,
            entity_revision=1,
            effective_at=day(n),
            source_observed_at=day(n if observed is None else observed),
        ),
        **payload,
    }


def line(deliveries=(0,), ordered=None, canceled=0, terminal=True):
    """Terminal by default; explicit source coverage spans all allocation windows."""
    records = [
        event(
            "commitment",
            "commit",
            -1,
            ordered_basis_qty=ordered or len(deliveries),
            canceled_unshipped_qty=canceled,
            fulfillment_terminal=terminal,
        )
    ]
    for i, n in enumerate(deliveries):
        records += [
            event("allocation", f"a{i}", -1, allocation_id=f"a{i}", quantity=1),
            event(
                "delivery",
                f"d{i}",
                n,
                allocation_id=f"a{i}",
                delivered_qty=1,
                actual_delivered_at=day(n),
            ),
        ]
    for feed in ("commitment", "allocation", "delivery", "return", "refund"):
        records.append(
            event(
                "coverage",
                f"cov-{feed}",
                0,
                observed=60,
                covered_kind=feed,
                status="complete",
                receipt_status="complete",
                evidence_ref="manifest",
                interval_start=day(-10),
                interval_end=day(100),
            )
        )
    return dict(line_id="line", dataset_version="dataset", seller_sku="sku", events=records)


def approve(data, n, allocation="a0", observed=None, eid="approval"):
    """Append an approval event; request time cannot stand in for qualification."""
    data["events"].append(
        event(
            "return",
            eid,
            n,
            observed=observed,
            allocation_id=allocation,
            event_type="approved",
            quantity=1,
        )
    )
    return data


def refund(data, n, allocation="a0", status="success", component="product", eid="refund"):
    """Append a product/shipping refund with explicit allocation linkage."""
    data["events"].append(
        event(
            "refund",
            eid,
            n,
            allocation_id=allocation,
            status=status,
            component=component,
            amount_minor=100,
        )
    )
    return data


def outcome(data, state="labeled", value=0, cutoff=70):
    """Assert contract state, nullable label, and policy ID through the public seam."""
    result = label_line(data, day(cutoff))
    assert (result.state, result.label) == (state, value)
    assert result.label_version == "return-or-refund-line-v2"
    return result


def test_L01_second_allocation_return():
    """L01: 1; cohort D20; maturity D50 (literal contract expected)."""
    result = outcome(approve(line((0, 20)), 45, "a1"), value=1)
    assert result.cohort_date == day(20).date()
    assert result.maturity_end == day(50)


def test_L02_first_allocation_expired():
    """L02: 0, even though event falls within the last allocation's window."""
    outcome(approve(line((0, 20)), 45))


def test_L03_approval_not_request_time():
    """L03: 0; request D29 and approval D31."""
    data = approve(line(), 31)
    data["events"].append(
        event("return", "request", 29, event_type="requested", quantity=1, allocation_id="a0")
    )
    outcome(data)


def test_L04_product_success_at_deadline():
    """L04: 1 at inclusive D30 boundary."""
    outcome(refund(line(), 30), value=1)


def test_L05_pending_failed_then_late_success():
    """L05: 0; pending/failed do not qualify."""
    data = refund(
        refund(
            refund(line(), 28, status="pending", eid="pending"), 29, status="failed", eid="failed"
        ),
        32,
    )
    outcome(data)


def test_L06_shipping_only():
    """L06: 0; shipping-only refund is not product refund."""
    outcome(refund(line(), 10, component="shipping"))


def test_L07_partial_cancel_single_sample():
    """L07: 0; delivered1/canceled2, cohort D0."""
    result = outcome(line(ordered=3, canceled=2))
    assert result.cohort_date == day(0).date()


def test_L08_unfinished_despite_positive():
    """L08: immature, positive and negative use the same terminal gate."""
    outcome(approve(line(ordered=3, terminal=False), 5), "immature", None)


def test_L09_refund_missing_allocation():
    """L09: unknown, do not guess which allocation window applies."""
    outcome(refund(line((0, 20)), 25, allocation=None), "unknown", None)


def test_L10_late_approval_incomplete_feed():
    """L10: unknown/provisional chosen as unknown at C=D31."""
    outcome(approve(line(), 29, observed=35), "unknown", None, cutoff=31)


def test_L11_late_approval_complete_feed():
    """L11: 1 at D40; coverage is independently observed at D39."""
    data = approve(line(), 29, observed=35)
    for r in data["events"]:
        if r["kind"] == "coverage":
            r["source_observed_at"] = day(39)
    outcome(data, value=1, cutoff=40)


def test_L12_business_cancellation_preserves_approval():
    """L12: 1; process cancellation is not a source correction."""
    data = approve(line(), 10)
    data["events"].append(event("return", "cancel", 12, event_type="canceled", allocation_id="a0"))
    outcome(data, value=1)


def test_L13_observed_source_correction_generations():
    """L13: assume coverage known D31; explicit invalidation observed D40 replaces approval.

    Immutable generation is represented by retaining the first frozen result.
    """
    data = approve(line(), 10)
    for r in data["events"]:
        if r["kind"] == "coverage":
            r["source_observed_at"] = day(31)
    data["events"].append(
        event(
            "return",
            "fix",
            10,
            observed=40,
            event_type="approved",
            quantity=1,
            allocation_id="a0",
            invalidated=True,
            corrects_source_event_id="approval",
            operation="correction",
        )
    )
    old = outcome(data, value=1, cutoff=35)
    outcome(data, value=0, cutoff=41)
    assert old.label == 1


def test_L14_full_cancel():
    """L14: ineligible, label null."""
    outcome(line((), ordered=3, canceled=3), "ineligible", None)


def test_L15_cutoff_equals_maturity():
    """L15: immature, C must strictly exceed maturity_end."""
    outcome(line(), "immature", None, cutoff=30)


def test_L16_order_scope_then_resolved_to_A():
    """L16: assume common order; correction D65 identifies A/a0, B becomes 0."""
    a = line()
    a.update(line_id="A", native_order_id="order", native_line_id="A")
    b = deepcopy(a)
    b.update(line_id="B", native_line_id="B")
    for data in (a, b):
        data["events"].append(
            event(
                "refund",
                "unmatched",
                10,
                allocation_id=None,
                component="product",
                status="success",
                amount_minor=100,
                scope={"native_order_id": "order"},
            )
        )
        outcome(data, "unknown", None, cutoff=64)
        # A correction tombstone removes old scope on both candidates; replacement
        # event carries the narrowed scope. Tombstone is not itself qualifying.
        data["events"].append(
            event(
                "refund",
                "resolve",
                10,
                observed=65,
                corrects_source_event_id="unmatched",
                invalidated=True,
            )
        )
        data["events"].append(
            event(
                "refund",
                "resolved",
                10,
                observed=65,
                allocation_id="a0",
                component="product",
                status="success",
                amount_minor=100,
                scope={"line_id": "A"},
            )
        )
    outcome(a, value=1)
    outcome(b, value=0)


def test_L17_snapshot_restatement_and_training_checksum(tmp_path):
    """L17: assume coverage D31, late event D35, corrected delivery D45 at D50.

    Training export is serialized before revisions; its bytes/checksum stay unchanged.
    """
    import hashlib
    import json
    from dataclasses import asdict

    data = line()
    for r in data["events"]:
        if r["kind"] == "coverage":
            r["source_observed_at"] = day(31)
    initial = outcome(data, cutoff=32)
    frozen = json.dumps(asdict(initial), default=str, sort_keys=True).encode()
    training_export = tmp_path / "training-generation-0.json"
    training_export.write_bytes(frozen)
    digest = hashlib.sha256(training_export.read_bytes()).hexdigest()
    approve(data, 10, observed=35)
    outcome(data, value=1, cutoff=40)
    data["events"].append(
        event(
            "delivery",
            "delivery-fix",
            45,
            observed=50,
            entity_revision=2,
            corrects_source_event_id="d0",
            allocation_id="a0",
            delivered_qty=1,
            actual_delivered_at=day(45),
        )
    )
    outcome(data, "immature", None, cutoff=60)
    outcome(data, value=0, cutoff=32)
    assert hashlib.sha256(training_export.read_bytes()).hexdigest() == digest
    assert json.loads(training_export.read_bytes())["label"] == 0


@pytest.mark.parametrize("offset,expected", [(-1, 1), (0, 1), (1, 0)])
def test_window_microsecond_boundary(offset, expected):
    """Deadline ±1 microsecond; expected literals come from the inclusive contract."""
    data = refund(line(), 30)
    data["events"][-1]["effective_at"] += timedelta(microseconds=offset)
    outcome(data, value=expected)


def test_observed_exactly_cutoff_is_invisible():
    """Exclusive snapshot excludes a correction observed exactly at C."""
    data = approve(line(), 10)
    data["events"].append(
        event(
            "return", "fix", 10, observed=70, invalidated=True, corrects_source_event_id="approval"
        )
    )
    outcome(data, value=1, cutoff=70)
    outcome(data, value=0, cutoff=71)


def test_feature_excludes_mature_label_known_later():
    """Coverage first known D60: mature D30 cannot enter a D50 feature."""
    target = dict(dataset_version="dataset", line_id="target", seller_sku="sku", created_at=day(50))
    assert sku_return_feature(target, [approve(line(), 10)]) == {
        "labeled_count_90d": 0,
        "positive_count_90d": 0,
        "return_or_refund_rate_90d": None,
        "label_version": "return-or-refund-line-v2",
    }
    target["created_at"] = day(65)
    assert sku_return_feature(target, [approve(line(), 10)])["return_or_refund_rate_90d"] == 1.0


def test_feature_recomputes_before_future_correction():
    """Future invalidation cannot overwrite the positive known at order time."""
    data = approve(line(), 10)
    data["events"].append(
        event(
            "return", "fix", 10, observed=80, invalidated=True, corrects_source_event_id="approval"
        )
    )
    target = dict(dataset_version="dataset", line_id="target", seller_sku="sku", created_at=day(65))
    assert sku_return_feature(target, [data])["positive_count_90d"] == 1
    target["created_at"] = day(85)
    assert sku_return_feature(target, [data])["positive_count_90d"] == 0


def test_refund_money_reversal_keeps_historical_success():
    """Ordinary money reversal does not erase a successful product refund."""
    data = refund(line(), 10)
    data["events"].append(
        event(
            "refund",
            "reversal",
            12,
            status="reversed",
            component="product",
            amount_minor=100,
            allocation_id="a0",
        )
    )
    outcome(data, value=1)


def test_missing_coverage_even_positive_is_unknown():
    """A positive event cannot stand in for complete feed evidence."""
    data = approve(line(), 10)
    data["events"] = [r for r in data["events"] if r.get("covered_kind") != "refund"]
    outcome(data, "unknown", None)


def test_feature_cohort_ninety_day_boundary_and_other_sku():
    """Cohort D0 counts at D90, expires D91; another SKU never contributes."""
    target = dict(dataset_version="dataset", line_id="target", seller_sku="sku", created_at=day(90))
    history = [approve(line(), 10)]
    assert sku_return_feature(target, history)["labeled_count_90d"] == 1
    target["created_at"] = day(91)
    assert sku_return_feature(target, history)["labeled_count_90d"] == 0
    target.update(created_at=day(90), seller_sku="other")
    assert sku_return_feature(target, history)["labeled_count_90d"] == 0


def test_source_scope_narrows_correction_without_leaving_B_unknown():
    """One real narrowed correction removes the old order-level uncertainty for B."""
    data = line()
    data.update(line_id="B", native_order_id="order")
    data["events"].append(
        event(
            "refund",
            "unmatched",
            10,
            component="product",
            status="success",
            amount_minor=100,
            scope={"native_order_id": "order"},
        )
    )
    data["events"].append(
        event(
            "refund",
            "fix-A",
            10,
            observed=65,
            component="product",
            status="success",
            amount_minor=100,
            allocation_id="a0",
            corrects_source_event_id="unmatched",
            scope={"line_id": "A", "native_order_id": "order"},
        )
    )
    outcome(data, "unknown", None, cutoff=64)
    outcome(data, value=0)


def test_unavailable_or_missing_manifest_is_not_coverage():
    """An unavailable scope or unreferenced claim cannot certify an official negative."""
    data = line()
    for r in data["events"]:
        if r.get("covered_kind") == "delivery":
            r["status"] = "unavailable"
    outcome(data, "unknown", None)
    for r in data["events"]:
        if r["kind"] == "coverage":
            r["status"] = "complete"
            r["evidence_ref"] = None
    outcome(data, "unknown", None)


def test_conflicting_event_identity_is_unknown():
    """A source event ID with contradictory approval payload cannot yield label 0/1."""
    data = approve(line(), 10)
    data["events"].append({**data["events"][-1], "quantity": 0})
    outcome(data, "unknown", None)


def test_raw_envelope_adapter_validates_coverage_digest():
    """Hand-built source manifests certify a negative; silent byte corruption invalidates it."""
    import hashlib
    import json

    from spark_jobs.lib.return_label import FEED_KINDS, source_line

    source = line()
    raw = []
    feeds = {v: k for k, v in FEED_KINDS.items()}
    for r in source["events"]:
        if r["kind"] == "coverage":
            continue
        raw.append(
            dict(
                dataset_version="dataset",
                feed=feeds[r["kind"]],
                producer="seller",
                source_epoch="epoch",
                source_partition="p",
                source_sequence=len(raw) + 1,
                source_event_id=r["source_event_id"],
                entity_id=r["entity_id"],
                entity_revision=1,
                effective_at=r["effective_at"].isoformat(),
                source_observed_at=r["source_observed_at"].isoformat(),
                payload={
                    k: v.isoformat() if isinstance(v, datetime) else v
                    for k, v in r.items()
                    if k
                    not in (
                        "kind",
                        "effective_at",
                        "source_observed_at",
                        "entity_id",
                        "entity_revision",
                        "source_event_id",
                    )
                },
            )
        )
    for kind, feed in feeds.items():
        rows = sorted(
            [r for r in raw if r.get("feed") == feed and "payload" in r],
            key=lambda r: r["source_event_id"],
        )
        digest = hashlib.sha256(
            json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        raw.append(
            dict(
                dataset_version="dataset",
                producer="seller",
                source_epoch="epoch",
                feed=feed,
                coverage_id="coverage-" + kind,
                revision=1,
                status="complete",
                profile_id="observed-operational",
                capability=kind,
                scope={},
                source_observed_at=day(60).isoformat(),
                source_record_count=len(rows),
                record_set_sha256=digest,
                event_interval=dict(
                    start_inclusive=day(-10).isoformat(), end_exclusive=day(100).isoformat()
                ),
                source_sequence_ranges=[
                    dict(partition="p", first=r["source_sequence"], last=r["source_sequence"])
                    for r in rows
                ],
                expected_partitions=["p"],
                completed_partitions=["p"],
                unresolved_scope_refs=[],
            )
        )
    identity = dict(line_id="line", dataset_version="dataset", seller_sku="sku")
    outcome(source_line(identity, raw))
    raw[0]["payload"]["ordered_basis_qty"] = 2
    outcome(source_line(identity, raw), "immature", None)
    raw[0]["payload"]["ordered_basis_qty"] = 1
    raw[0]["payload"]["currency"] = "EUR"
    outcome(source_line(identity, raw), "unknown", None)


def test_no_delivery_is_ineligible_per_contract():
    """Contract says no delivery → ineligible, even if undelivered fulfillment is open."""
    outcome(line((), ordered=3, terminal=False), "ineligible", None)


@pytest.mark.parametrize(
    "build, kind",
    [
        (lambda: refund(approve(line(), 10), 12), "return_and_refund"),
        (lambda: refund(line(), 12), "refund_only"),
        (lambda: approve(line(), 10), "return_only"),
        (lambda: line(), None),
    ],
)
def test_positive_kind_separates_return_and_refund(build, kind):
    """Return ≠ Refund: label stays 1 for all three positive cases, kind tells which; null when 0."""
    result = label_line(build(), day(70))
    assert result.state == "labeled"
    assert result.label == (0 if kind is None else 1)
    assert result.positive_kind == kind


def test_positive_kind_ignores_refund_outside_window():
    """A product refund after the 30-day window neither qualifies nor turns return_only into a pair."""
    result = label_line(refund(approve(line(), 10), 45), day(70))
    assert (result.label, result.positive_kind) == (1, "return_only")


@pytest.mark.parametrize(
    "mode, expected",
    [
        ("contiguous", "labeled"),
        ("gap", "unknown"),
        ("partial", "unknown"),
        ("receipt-partial", "unknown"),
        ("unresolved", "unknown"),
        ("exclusive-end", "unknown"),
    ],
)
def test_contiguous_coverage_union(mode, expected):
    """L18–L20: contiguous union is enough; gaps, partial, unresolved and the end edge fail closed."""
    # Delivery window [D0,D30]; the tail D28–D35 needs two contiguous weeks to cover the deadline.
    data = line()
    data["events"] = [r for r in data["events"] if r["kind"] != "coverage"]
    for kind in ("commitment", "allocation", "delivery", "return", "refund"):
        for week in range(5):
            if mode == "gap" and kind == "refund" and week == 2:
                continue
            r = event(
                "coverage",
                f"{kind}-{week}",
                observed=36,
                covered_kind=kind,
                status="complete",
                receipt_status="complete",
                evidence_ref="receipt",
                interval_start=day(week * 7),
                interval_end=day((week + 1) * 7),
            )
            if kind == "refund" and week == 2:
                if mode == "partial":
                    r["status"] = "partial"
                if mode == "receipt-partial":
                    r["receipt_status"] = "partial"
                if mode == "unresolved":
                    r["unresolved_scope_refs"] = ["u"]
            if mode == "exclusive-end" and week == 4:
                r["interval_end"] = day(30)
            data["events"].append(r)
    assert label_line(data, day(40)).state == expected
