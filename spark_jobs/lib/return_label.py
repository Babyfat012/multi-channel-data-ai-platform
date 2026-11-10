"""Return label rules v2, independent of Spark and of the simulator's ground truth.

Input: the identity of one line and `events`, the normalized source observations (`kind`, event
identity, revision, `effective_at`, `source_observed_at`, payload). All clocks are UTC.

- Coverage: needs checked source envelopes covering the whole delivery window (several contiguous
  envelopes allowed). Missing authority, links or evidence returns `unknown` (fail closed).
- Ordinary cancellations/reversals keep history; an explicit correction only replaces observations from
  the time it was observed.
- Ambiguous reversals, partial deliveries at different times, conflicting revisions: `unknown`.
- `decision_observed_at` counts every related observation seen, without guessing a minimal set.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any

LABEL_VERSION = "return-or-refund-line-v2"
WINDOW = timedelta(days=30)
REQUIRED_KINDS = ("commitment", "allocation", "delivery", "return", "refund")


@dataclass(frozen=True)
class LabelResult:
    """Label result (possibly null) and the evidence to re-check when it became available."""

    state: str
    label: int | None
    cohort_date: date | None
    maturity_end: datetime | None
    qualifying_event_id: str | None = None
    reasons: list[str] = field(default_factory=list)
    label_version: str = LABEL_VERSION
    decision_observed_at: datetime | None = None
    source_event_refs: list[str] = field(default_factory=list)
    # Three post-sale cases when label = 1 (Return ≠ Refund): return_and_refund / refund_only / return_only; else None.
    positive_kind: str | None = None


def utc(value: str | datetime) -> datetime:
    """Source ISO string or Spark's naive UTC datetime -> time-zone-aware UTC datetime."""
    parsed = (
        datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    )
    return (
        parsed.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None
        else parsed.astimezone(timezone.utc)
    )


def scope_matches(scope: Mapping[str, Any], line: Mapping[str, Any]) -> bool:
    """Keep every line that may be affected; narrow only when the source declares an explicit scope."""
    for key in (
        "dataset_version",
        "channel",
        "account_id",
        "native_order_id",
        "line_id",
        "native_line_id",
    ):
        if scope.get(key) is not None and scope[key] != line.get(key):
            return False
        plural = {
            "channel": "channels",
            "account_id": "account_ids",
            "native_line_id": "candidate_native_line_ids",
        }.get(key, key + "s")
        if scope.get(plural) and line.get(key) not in scope[plural]:
            return False
    return True


def visible_events(line: Mapping[str, Any], cutoff: str | datetime) -> list[dict]:
    """Filter by observation snapshot (cutoff exclusive) and scope, before handling revisions."""
    observed = [
        dict(r)
        for r in line.get("events", [])
        if r.get("source_observed_at") is not None and utc(r["source_observed_at"]) < utc(cutoff)
    ]
    result = []
    matched_ids = {
        r.get("source_event_id")
        for r in observed
        if scope_matches(r.get("scope") or r.get("affected_scope") or {}, line)
    }
    for record in observed:
        scope = record.get("scope") or record.get("affected_scope") or {}
        if scope_matches(scope, line):
            result.append(record)
        elif (
            record.get("corrects_source_event_id")
            and record["corrects_source_event_id"] in matched_ids
        ):
            # The old scope must see that the unmatched event was resolved,
            # even when the replacement now narrows to another line.
            result.append({**record, "invalidated": True, "scope": {}, "affected_scope": {}})
    return result


def _resolved(records: list[dict]) -> tuple[list[dict], bool]:
    """Apply observed corrections and status revisions; keep process events in history."""
    by_id = {}
    conflict = False
    for record in records:
        eid = record.get("source_event_id")
        if eid in by_id and record != by_id[eid]:
            conflict = True
        by_id[eid] = record
    corrected = {
        r["corrects_source_event_id"] for r in records if r.get("corrects_source_event_id")
    }
    kept = [r for r in by_id.values() if r.get("source_event_id") not in corrected]
    latest = {}
    history = []
    for record in kept:
        if record["kind"] in ("return", "refund"):
            history.append(record)
            continue
        key = (
            record["kind"],
            record.get("producer"),
            record.get("source_epoch"),
            record.get("entity_id", record.get("source_event_id")),
        )
        revision = record.get("entity_revision", record.get("revision", 1))
        prior = latest.get(key)
        if (
            prior
            and revision == prior.get("entity_revision", prior.get("revision", 1))
            and record != prior
        ):
            conflict = True
        if prior is None or revision > prior.get("entity_revision", prior.get("revision", 1)):
            latest[key] = record
    return list(latest.values()) + history, conflict


def _first_uncovered_kind(records, first_delivery: datetime, maturity: datetime) -> str | None:
    """First feed whose valid, contiguous coverage chain does NOT fully cover `[first_delivery, maturity]`.

    Contiguous coverage (ADR-0013): starting from the first delivery, each envelope starting no later than
    the covered point extends the coverage. A gap stops it. Coverage end is exclusive, so it must
    EXCEED `maturity`.
    """
    coverage = [r for r in records if r["kind"] == "coverage"]
    for kind in REQUIRED_KINDS:
        valid = [
            r
            for r in coverage
            if r.get("covered_kind") == kind
            and r.get("status") == "complete"
            and r.get("receipt_status") == "complete"
            and r.get("evidence_ref")
            and r.get("interval_start")
            and r.get("interval_end")
            and not r.get("unresolved_scope_refs")
        ]
        covered_until = first_delivery
        for envelope in sorted(valid, key=lambda r: utc(r["interval_start"])):
            begin, end = utc(envelope["interval_start"]), utc(envelope["interval_end"])
            if begin > covered_until:
                break
            covered_until = max(covered_until, end)
        if covered_until <= maturity:
            return kind
    return None


def label_line(line: Mapping[str, Any], cutoff: str | datetime) -> LabelResult:
    """Compute the official label of a line at the cutoff (exclusive), using only observed evidence.

    Not delivered (including still-open lines) is `ineligible`. Missing commitment, identity, authority,
    feed, or an ambiguous allocation is `unknown`. Unknown fulfillment is considered before a positive label.
    Coverage end is exclusive, so it must exceed every deadline. Positive and negative labels wait for the same evidence.
    """
    cutoff = utc(cutoff)
    visible = visible_events(line, cutoff)
    refs = sorted({r["source_event_id"] for r in visible if r.get("source_event_id")})
    known_at = max((utc(r["source_observed_at"]) for r in visible), default=None)
    cohort = maturity = None

    def result(state, value=None, reason=None, eid=None):
        """Build an immutable result, with the shared snapshot evidence."""
        return LabelResult(
            state,
            value,
            cohort,
            maturity,
            eid,
            [reason] if reason else [],
            LABEL_VERSION,
            known_at,
            refs,
        )

    records, conflict = _resolved(visible)
    if conflict:
        return result("unknown", reason="conflicting_source_revision")
    groups = {kind: [r for r in records if r["kind"] == kind] for kind in REQUIRED_KINDS}
    if len(groups["commitment"]) != 1:
        return result("unknown", reason="missing_or_conflicting_commitment")
    commitment = groups["commitment"][0]
    ordered = commitment.get("ordered_basis_qty")
    canceled = commitment.get("canceled_unshipped_qty")
    if ordered is None or canceled is None or ordered <= 0 or canceled < 0 or canceled > ordered:
        return result("unknown", reason="invalid_commitment_quantity")
    deliveries = groups["delivery"]
    delivered = sum(r.get("delivered_qty", 0) for r in deliveries)
    if not delivered:
        if canceled == ordered and commitment.get("fulfillment_terminal"):
            return result("ineligible", reason="fully_canceled_before_delivery")
        return result("ineligible", reason="no_delivery")
    allocations = {r.get("allocation_id"): r for r in groups["allocation"]}
    windows = {}
    quantities = {}
    for delivery in deliveries:
        aid = delivery.get("allocation_id")
        if (
            aid is None
            or aid not in allocations
            or not delivery.get("actual_delivered_at")
            or delivery.get("delivered_qty", 0) <= 0
        ):
            return result("unknown", reason="missing_delivery_allocation")
        d = utc(delivery["actual_delivered_at"])
        if aid in windows and windows[aid] != d:
            return result("unknown", reason="allocation_has_multiple_delivery_times")
        windows[aid] = d
        quantities[aid] = quantities.get(aid, 0) + delivery["delivered_qty"]
    maturity = max(windows.values()) + WINDOW
    cohort = max(windows.values()).date()
    if delivered + canceled > ordered or any(
        quantities[a] > allocations[a].get("quantity", 0) for a in quantities
    ):
        return result("unknown", reason="conflicting_quantity")
    if (
        not commitment.get("fulfillment_terminal")
        or delivered + canceled < ordered
        or any(
            quantities.get(a, 0) < r.get("quantity", 0)
            for a, r in allocations.items()
            if r.get("allocation_state") != "canceled"
        )
    ):
        return result("immature", reason="fulfillment_not_terminal")
    if maturity >= cutoff:
        return result("immature", reason="window_not_closed")
    for r in records:
        if r.get("authority_valid") is False or r.get("ambiguous_reversal"):
            return result("unknown", reason="authority_or_reversal_conflict")
        if r["kind"] == "uncertainty" and r.get("resolution_state") != "resolved":
            return result("unknown", reason="unresolved_affected_scope")
    missing = _first_uncovered_kind(records, min(windows.values()), maturity)
    if missing:
        return result("unknown", reason=f"missing_complete_coverage:{missing}")
    qualifying = []
    kinds = set()
    for r in groups["return"] + groups["refund"]:
        positive = (
            r["kind"] == "return" and r.get("event_type") == "approved" and r.get("quantity", 0) > 0
        ) or (
            r["kind"] == "refund"
            and r.get("status") == "success"
            and r.get("component") == "product"
            and r.get("amount_minor", 0) > 0
        )
        if not positive or r.get("invalidated"):
            continue
        aid = r.get("allocation_id")
        if aid not in windows:
            return result("unknown", reason="qualifying_event_missing_allocation")
        if r["kind"] == "return" and r["quantity"] > quantities[aid]:
            return result("unknown", reason="return_quantity_exceeds_delivery")
        if windows[aid] <= utc(r["effective_at"]) <= windows[aid] + WINDOW:
            qualifying.append(r["source_event_id"])
            kinds.add(r["kind"])
    outcome = result("labeled", int(bool(qualifying)), eid=min(qualifying) if qualifying else None)
    # Pair Return–Refund by line + window, not yet by return_id (not every Channel sends it).
    positive_kind = (
        {"return", "refund"} == kinds
        and "return_and_refund"
        or kinds == {"refund"}
        and "refund_only"
        or kinds == {"return"}
        and "return_only"
        or None
    )
    return replace(outcome, positive_kind=positive_kind)


def sku_return_feature(target: Mapping[str, Any], history: list[Mapping[str, Any]]) -> dict:
    """Per-SKU feature over a 90-day UTC cohort [T.date-90, T.date), recomputing labels at T.

    History input is source events, not final labels or simulator ground truth. Recomputing at
    T avoids late-arriving events or future corrections.
    """
    timestamp = utc(target["created_at"])
    values = []
    for candidate in history:
        if (
            candidate.get("dataset_version") != target.get("dataset_version")
            or candidate.get("seller_sku") != target.get("seller_sku")
            or candidate.get("line_id") == target.get("line_id")
        ):
            continue
        result = label_line(candidate, timestamp)
        if (
            result.state == "labeled"
            and result.maturity_end < timestamp
            and result.decision_observed_at < timestamp
            and timestamp.date() - timedelta(days=90) <= result.cohort_date < timestamp.date()
        ):
            values.append(result.label)
    return dict(
        labeled_count_90d=len(values),
        positive_count_90d=sum(values),
        return_or_refund_rate_90d=sum(values) / len(values) if values else None,
        label_version=LABEL_VERSION,
    )


FAR_FUTURE = datetime(9999, 1, 1, tzinfo=timezone.utc)


def label_segments(line: Mapping[str, Any]) -> list[dict]:
    """Every cutoff range `(start, end]` in which the line is `labeled`, with its result.

    `label_line` only reads the cutoff in two places: `observed < cutoff` and `maturity >= cutoff`. Between two
    consecutive observation times x_i < x_{i+1}, the visible event set does not change, so the result
    is constant on (max(x_i, maturity_i), x_{i+1}]. Computing once at the upper end of each segment is enough:
    linear instead of recomputing for every (line, order time) pair. `end=None` is the open last segment.
    """
    times = sorted(
        {
            utc(r["source_observed_at"])
            for r in line.get("events", [])
            if r.get("source_observed_at") is not None
        }
    )
    segments = []
    for index, start in enumerate(times):
        end = times[index + 1] if index + 1 < len(times) else None
        result = label_line(line, end or FAR_FUTURE)
        if result.state != "labeled":
            continue
        lower = max(start, result.maturity_end)
        if end is not None and lower >= end:
            continue
        segments.append(
            dict(
                start_exclusive=lower,
                end_inclusive=end,
                label=result.label,
                cohort_date=result.cohort_date,
                maturity_end=result.maturity_end,
                decision_observed_at=result.decision_observed_at,
                qualifying_event_id=result.qualifying_event_id,
            )
        )
    return segments


def feature_window(segment: Mapping[str, Any]) -> tuple[datetime, datetime] | None:
    """Order-time range [a, b) in which a labeled segment counts toward the PIT feature.

    Counted at t when t is in (start, end] and the cohort day is in [t.date-90, t.date), i.e. t is in
    [cohort+1 day, cohort+91 days) at 00:00 UTC. Timestamps are exact to the microsecond, so
    (start, end] == [start+1µs, end+1µs).
    """
    micro = timedelta(microseconds=1)
    cohort = datetime.combine(segment["cohort_date"], datetime.min.time(), tzinfo=timezone.utc)
    begin = max(utc(segment["start_exclusive"]) + micro, cohort + timedelta(days=1))
    end = cohort + timedelta(days=91)
    if segment["end_inclusive"] is not None:
        end = min(end, utc(segment["end_inclusive"]) + micro)
    return (begin, end) if begin < end else None


FEED_KINDS = {
    "seller_oms.line_commitments": "commitment",
    "seller_oms.fulfillment_allocations": "allocation",
    "carrier.delivery_events": "delivery",
    "seller_postsales.return_events": "return",
    "seller_postsales.refund_events": "refund",
}


def _coverage_verified(raw: Mapping[str, Any], decoded: list[dict]) -> bool:
    """Self-check a coverage envelope against the WHOLE observed feed (digest, row count, sequence, partition)."""
    import hashlib
    import json

    feed = raw.get("feed")
    observed = raw.get("source_observed_at")
    interval = raw.get("event_interval") or {}
    rows = [
        r
        for r in decoded
        if r.get("source_event_id")
        and r.get("feed") == feed
        and r.get("producer") == raw.get("producer")
        and r.get("source_epoch") == raw.get("source_epoch")
        and utc(r["source_observed_at"]) < utc(observed)
        and interval.get("start_inclusive")
        and interval.get("end_exclusive")
        and utc(interval["start_inclusive"])
        <= utc(r["effective_at"])
        < utc(interval["end_exclusive"])
    ]
    canonical = json.dumps(
        sorted(rows, key=lambda r: r["source_event_id"]),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    declared = {
        (item["partition"], n)
        for item in raw.get("source_sequence_ranges", [])
        for n in range(item["first"], item["last"] + 1)
    }
    actual = {(r.get("source_partition"), r.get("source_sequence")) for r in rows}
    valid = (
        raw.get("status") == "complete"
        and digest == raw.get("record_set_sha256")
        and len(rows) == raw.get("source_record_count")
        and declared == actual
        and sorted(raw.get("expected_partitions", []))
        == sorted(raw.get("completed_partitions", []))
        and bool(raw.get("capability"))
        and bool(raw.get("profile_id"))
        and interval.get("start_inclusive") is not None
        and interval.get("end_exclusive") is not None
    )
    if valid:
        valid = all(
            utc(interval["start_inclusive"])
            <= utc(r["effective_at"])
            < utc(interval["end_exclusive"])
            for r in rows
        )
    return valid


def _coverage_record(
    raw: Mapping[str, Any], decoded: list[dict], coverage_status: Mapping[str, str] | None
) -> dict:
    """Coverage envelope → `coverage` record; status comes from Silver if present, otherwise self-checked."""
    event_id = raw["coverage_id"] + ":" + str(raw["revision"])
    if coverage_status is not None:
        status = coverage_status.get(event_id, "partial")
    else:
        status = "complete" if _coverage_verified(raw, decoded) else "partial"
    interval = raw.get("event_interval") or {}
    return {
        **raw,
        "kind": "coverage",
        "source_event_id": event_id,
        "entity_id": raw["coverage_id"],
        "covered_kind": FEED_KINDS[raw.get("feed")],
        "entity_revision": raw["revision"],
        "evidence_ref": raw["coverage_id"],
        "receipt_status": status,
        "interval_start": interval.get("start_inclusive"),
        "interval_end": interval.get("end_exclusive"),
    }


def _operational_record(
    raw: Mapping[str, Any], decoded: list[dict], identity: Mapping[str, Any]
) -> dict:
    """Business feed event → flat record (envelope + payload), with scope and authority."""
    feed = raw.get("feed")
    payload = raw.get("payload") or {}
    scope = {
        key: payload[key]
        for key in ("line_id", "native_order_id", "native_line_id", "channel", "account_id")
        if payload.get(key) is not None
    }
    record = {**raw, **payload, "scope": scope, "kind": FEED_KINDS[feed]}
    # Coverage declares authority for exactly the operational feed/producer/epoch;
    # another producer may not borrow evidence from a different feed.
    record["authority_valid"] = any(
        c.get("coverage_id")
        and c.get("feed") == feed
        and c.get("producer") == raw.get("producer")
        and c.get("source_epoch") == raw.get("source_epoch")
        and c.get("capability")
        and c.get("profile_id")
        and scope_matches(c.get("scope") or {}, identity)
        for c in decoded
    )
    if record["kind"] == "refund":
        money = payload.get("allocation_money_rows", [])
        if len(money) == 1 and money[0].get("amount_minor") == payload.get("amount_minor"):
            record["allocation_id"] = money[0]["allocation_id"]
        elif money:
            # A Refund across several allocations needs effective evidence for each allocation.
            record["allocation_id"] = None
    return record


def _relevant_coverage(events: list[dict], identity: Mapping[str, Any]) -> list[dict]:
    """Keep only coverage that intersects the line's `[first delivery, last deadline]` (once Silver status exists).

    A superset across ALL delivery revisions: future corrections do not lose coverage.
    Authority is checked on the full decoded set, same feed/producer/epoch, before filtering.
    A line not yet delivered needs no coverage.
    """
    deliveries = [
        utc(r["actual_delivered_at"])
        for r in events
        if r["kind"] == "delivery"
        and r.get("actual_delivered_at")
        and scope_matches(r.get("scope") or {}, identity)
    ]
    if not deliveries:
        return [r for r in events if r["kind"] != "coverage"]
    begin, end = min(deliveries), max(deliveries) + WINDOW
    coverage = [r for r in events if r["kind"] == "coverage"]
    keep = {
        r["entity_id"]
        for r in coverage
        if not r.get("interval_start")
        or not r.get("interval_end")
        or (utc(r["interval_start"]) <= end and utc(r["interval_end"]) > begin)
    }
    # Keep every revision of a related envelope: a newer revision may narrow/move
    # the interval outside. Dropping it would revive the old revision in _resolved.
    changed = True
    while changed:
        refs = {r["source_event_id"] for r in coverage if r["entity_id"] in keep}
        additions = {r["entity_id"] for r in coverage if r.get("corrects_source_event_id") in refs}
        changed = bool(additions - keep)
        keep.update(additions)
    return [r for r in events if r["kind"] != "coverage" or r["entity_id"] in keep]


def source_line(
    identity: Mapping[str, Any],
    raw_records: list[str | dict],
    coverage_status: Mapping[str, str] | None = None,
) -> dict:
    """Turn observed source envelopes into pure input for the label rules, without hidden truth.

    Without `coverage_status` (small fixtures, unit tests): check digest/count/sequence/partition
    of the coverage right here over the whole feed, so `raw_records` must contain the full feed. With
    `coverage_status` ({coverage source_event_id: 'complete'|'partial'}, computed once by Silver
    in `coverage_validity`): only this line's records are needed, coverage is cut to the delivery window.

    Creation time is the `effective_at` of commitment revision 1; without it `created_at` is null.
    Authority comes from the capability/profile declared on the operational feed's coverage envelope.
    """
    import json

    decoded = [json.loads(r) if isinstance(r, str) else dict(r) for r in raw_records]
    decoded = [r for r in decoded if r.get("dataset_version") == identity["dataset_version"]]
    # An identical retry is not a new observation.
    unique = {json.dumps(r, sort_keys=True, separators=(",", ":")): r for r in decoded}
    decoded = list(unique.values())
    events = []
    created = []
    for raw in decoded:
        feed = raw.get("feed")
        if raw.get("coverage_id"):
            if feed not in FEED_KINDS:
                continue
            record = _coverage_record(raw, decoded, coverage_status)
        elif raw.get("uncertainty_id"):
            record = {
                **raw,
                "kind": "uncertainty",
                "entity_id": raw["uncertainty_id"],
                "source_event_id": raw["uncertainty_id"] + ":" + str(raw["revision"]),
                "entity_revision": raw["revision"],
            }
        elif feed in FEED_KINDS:
            record = _operational_record(raw, decoded, identity)
            if (
                record["kind"] == "commitment"
                and raw.get("entity_revision") == 1
                and scope_matches(record["scope"], identity)
            ):
                created.append(utc(raw["effective_at"]))
        else:
            continue
        events.append(record)
    if coverage_status is not None:
        events = _relevant_coverage(events, identity)
    return {**identity, "events": events, "created_at": min(created) if created else None}
