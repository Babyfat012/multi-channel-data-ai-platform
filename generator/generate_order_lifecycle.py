"""Generate diverse seeded v2 sources, buffering each day on disk, stdlib only.

Run ``python -m generator.generate_order_lifecycle --output DIR --orders-per-day 200
--days 10 --seed 7``. The truth file is for testing only and lives outside the source tree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import random
import resource
import sqlite3
import tempfile
import time
from bisect import bisect_left
from collections import Counter, OrderedDict, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from generator.generate_order_lifecycle_fixture import (
    CHANNELS,
    CLOCK,
    FEEDS,
    PROFILES,
    _emit_sources,
    _Emitter,
    _native,
    _stamp,
    canonical,
    digest,
    stable_id,
)

# Each line picks exactly one scenario; after-sales and correction use fixture payloads.
SCENARIO_WEIGHTS = {
    "delivered": 0.30,
    "partial_cancel": 0.10,
    "split_delivery": 0.12,
    # Return ≠ Refund (ADR-0010): the most common real-world case is a Return WITH a Refund.
    "return_with_refund": 0.07,
    "return_only": 0.03,
    "return_after_window": 0.07,
    "product_refund": 0.08,
    "refund_retry": 0.05,
    "late_observation": 0.08,
    "correction": 0.06,
    "right_censored": 0.04,
}
MAX_SOURCE_ROWS = 20_000_000
# Scenarios for label Return-or-Refund = 1 (returned/refunded within 30 days of delivery).
POSITIVE_SCENARIOS = (
    "return_with_refund",
    "return_only",
    "product_refund",
    "refund_retry",
    "correction",
)
# Only used when sku_return_skew > 0: Channels with prepaid labels/try-on return more easily (same direction as v1 returns.py).
CHANNEL_RETURN_FACTOR = {
    "amazon": 1.0,
    "tiktok_shop": 1.6,
    "shopify": 0.8,
    "walmart": 0.7,
    "shopee": 1.2,
}


@dataclass(frozen=True)
class ScaleConfig:
    """Immutable parameters; the horizon is days counted from start UTC, not the tail length."""

    seed: int = 7
    start: str = "2026-01-01T00:00:00.000000Z"
    days: int = 10
    orders_per_day: int = 200
    sku_count: int = 100
    sku_zipf_s: float = 1.2
    channel_weights: tuple[float, ...] = (1.0, 1.0, 1.0, 1.0, 1.0)
    lines_per_order: tuple[float, ...] = (0.55, 0.30, 0.12, 0.03)
    followup_days: int = 270
    observation_days: int = 285
    native_schemas: tuple[tuple[str, int], ...] = ()
    # 0 = labels depend on nothing (default, keeps old data unchanged). > 0: each SKU gets a Return factor
    # exp(skew·z), z ~ N(0,1) fixed per SKU, plus a per-Channel factor ⇒ the model has a signal to learn.
    sku_return_skew: float = 0.0

    @property
    def creation_days(self):
        """Expose the horizon name shared with the fixture emitter."""
        return self.days

    def validate(self):
        """Check every limit before creating directories or emitting sources."""
        for name, low, high in (
            ("days", 1, 180),
            ("orders_per_day", 1, 20000),
            ("sku_count", 2, 5000),
        ):
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{name} must be {low}..{high}")
        if type(self.seed) is not int:
            raise ValueError("seed must be int")
        if not math.isfinite(self.sku_zipf_s) or self.sku_zipf_s < 0:
            raise ValueError("sku_zipf_s must be finite and >= 0")
        if not math.isfinite(self.sku_return_skew) or not 0 <= self.sku_return_skew <= 3:
            raise ValueError("sku_return_skew must be 0..3")
        for weights, size, name in (
            (self.channel_weights, 5, "channel_weights"),
            (self.lines_per_order, 4, "lines_per_order"),
        ):
            if (
                len(weights) != size
                or any(not math.isfinite(x) or x < 0 for x in weights)
                or sum(weights) <= 0
            ):
                raise ValueError(f"invalid {name}")
        if (
            type(self.followup_days) is not int
            or type(self.observation_days) is not int
            or self.followup_days < self.days + 50
            or self.observation_days < self.followup_days + 9
        ):
            raise ValueError("horizons must cover creation + 50 days and observation + 9 days")
        start = datetime.fromisoformat(self.start.replace("Z", "+00:00"))
        if start.utcoffset() != timedelta(0):
            raise ValueError("start must be UTC")
        overrides = dict(self.native_schemas)
        if len(overrides) != len(self.native_schemas) or any(
            c not in CHANNELS
            or v not in (1, 2)
            or (v == 2 and c not in ("amazon", "shopee", "tiktok_shop"))
            for c, v in overrides.items()
        ):
            raise ValueError("unsupported native schema override")
        if self.row_upper_bound() > MAX_SOURCE_ROWS:
            raise ValueError("source row ceiling exceeds 20 million")
        return start

    def row_upper_bound(self):
        """Bound by the worst case of 24 rows/line, not by the sampling expectation."""
        max_lines = max(i + 1 for i, weight in enumerate(self.lines_per_order) if weight > 0)
        return (
            self.days * self.orders_per_day * (24 * max_lines + 1)
            + self.sku_count * 11
            + 32
            + len(FEEDS) * math.ceil(self.followup_days / 7)
        )

    def estimate(self):
        """Estimate rows/bytes from a pilot with the same renderer, at most 100 orders per day."""
        self.validate()
        pilot_orders = min(self.orders_per_day * self.days, 100)
        pilot = _pilot(self, pilot_orders)
        factor = self.days * self.orders_per_day / pilot_orders
        # Mappings are emitted once per (Channel, SKU); expected occupancy.
        mean_lines = sum((i + 1) * w for i, w in enumerate(self.lines_per_order)) / sum(
            self.lines_per_order
        )
        n = self.days * self.orders_per_day * mean_lines
        sku_weights = _sku_weights(self)
        total_sku = sum(sku_weights)
        total_ch = sum(self.channel_weights)
        mappings = sum(
            -math.expm1(n * math.log1p(-min(0.999999999999, sw / total_sku * cw / total_ch)))
            for sw in sku_weights
            for cw in self.channel_weights
            if cw > 0
        )
        weekly_rows = len(FEEDS) * math.ceil(self.followup_days / 7)
        base_rows = pilot["fixed_rows"] + mappings + 15 + weekly_rows
        rows = round(pilot["rows"] * factor + base_rows)
        size = round(
            pilot["bytes"] * factor
            + pilot["fixed_bytes"]
            + mappings * 1600
            + 20000
            + weekly_rows * 1600
        )
        return {
            "source_records": rows,
            "source_bytes": size,
            "source_record_upper_bound": self.row_upper_bound(),
            "method": "same-renderer pilot <=100 orders + catalog occupancy; approximate",
        }


def _sku_weights(config):
    """Zipf weights by stable rank; s=0 is uniform."""
    return [rank ** (-config.sku_zipf_s) for rank in range(1, config.sku_count + 1)]


def _params(config):
    """Parameters for the dataset id; drop `sku_return_skew` when = 0 so old data keeps its id/bytes."""
    params = asdict(config)
    if not params["sku_return_skew"]:
        del params["sku_return_skew"]
    return params


def _scenario_weights(config, sku, channel):
    """Scenario weights of a line: multiply the positive group by the SKU × Channel factor (per-SKU RNG)."""
    z = random.Random(f"{config.seed}:{sku}").gauss(0.0, 1.0)
    factor = math.exp(config.sku_return_skew * z) * CHANNEL_RETURN_FACTOR[channel]
    return tuple(w * factor if k in POSITIVE_SCENARIOS else w for k, w in SCENARIO_WEIGHTS.items())


def _orders_by_day(config):
    """Sample with a dedicated RNG and keep only the orders of exactly one day."""
    rng = random.Random(config.seed)
    start = datetime.fromisoformat(config.start.replace("Z", "+00:00"))
    population = stable_id("population", _params(config) | {"native_schemas": ()})
    sku_cdf, acc = [], 0.0
    for weight in _sku_weights(config):
        acc += weight
        sku_cdf.append(acc)
    for day in range(config.days):
        orders = []
        for number in range(config.orders_per_day):
            channel = rng.choices(CHANNELS, config.channel_weights)[0]
            created = start + timedelta(days=day, microseconds=rng.randrange(86400 * 1_000_000))
            oid = stable_id("native-order", population, day, number, channel)
            lines = []
            for index in range(rng.choices((1, 2, 3, 4), config.lines_per_order)[0]):
                if config.sku_return_skew:
                    # Draw the SKU FIRST so the scenario depends on the SKU; the default branch keeps the old order.
                    sku = f"sku-{bisect_left(sku_cdf, rng.random() * acc) + 1:05d}"
                    weights = _scenario_weights(config, sku, channel)
                    scenario = rng.choices(tuple(SCENARIO_WEIGHTS), weights)[0]
                else:
                    scenario = rng.choices(
                        tuple(SCENARIO_WEIGHTS), tuple(SCENARIO_WEIGHTS.values())
                    )[0]
                    sku = f"sku-{bisect_left(sku_cdf, rng.random() * acc) + 1:05d}"
                lid = stable_id("physical-line", population, day, number, index)
                units = [stable_id("unit", lid, u) for u in range(3)]
                canceled = int(scenario == "partial_cancel")
                split = scenario == "split_delivery"
                groups = [units[:2], units[2:]] if split else [units[: 3 - canceled]]
                postsales = scenario in (
                    "return_with_refund",
                    "return_only",
                    "return_after_window",
                    "product_refund",
                    "refund_retry",
                    "correction",
                )
                returning = scenario in (
                    "return_with_refund",
                    "return_only",
                    "return_after_window",
                    "correction",
                )
                approval = created + timedelta(days=34 if scenario == "return_after_window" else 32)
                line = {
                    "physical_line_id": lid,
                    "native_line_id": str(index + 1),
                    "sku": sku,
                    "quantity": 3,
                    "canceled": canceled,
                    "units": units,
                    "intent_unit": units[0] if postsales else None,
                    "ordered_net_minor": 30000,
                    "unit_gross_minor": 11000,
                    "unit_discount_minor": 1000,
                    "delivery_plan": [
                        {
                            "physical_unit_ids": group,
                            "ship_at": _stamp(created + timedelta(days=1 + part)),
                            "delivered_at": _stamp(created + timedelta(days=3 + part)),
                        }
                        for part, group in enumerate(groups)
                    ],
                    "scenario": scenario,
                    "postsales": postsales,
                    "return": returning,
                    "refund": scenario
                    in ("return_with_refund", "product_refund", "refund_retry", "correction"),
                    "refund_retry": scenario == "refund_retry",
                    "refund_correction": scenario == "correction",
                    "open": scenario == "right_censored",
                    "schedule": {"approved_at": _stamp(approval)},
                    "receive_at": _stamp(created + timedelta(days=40)),
                    "disposition_at": _stamp(created + timedelta(days=41)),
                    "correction_at": _stamp(created + timedelta(days=40)),
                    "late_seconds": rng.randint(3600, 4 * 86400)
                    if scenario == "late_observation"
                    else 0,
                }
                lines.append(line)
            orders.append(
                {
                    "channel": channel,
                    "account_id": f"account-{channel}-1",
                    "native_order_id": oid,
                    "created_at": _stamp(created),
                    "ship_at": _stamp(created + timedelta(days=1)),
                    "delivered_at": _stamp(created + timedelta(days=3)),
                    "approved_at": _stamp(created + timedelta(days=32)),
                    "lines": lines,
                }
            )
        yield orders


def _pilot(config, count):
    """Count lifecycle cost from a small sample, without opening files or a global RNG."""
    from dataclasses import replace

    cfg = replace(config, days=1, orders_per_day=count)
    orders = next(_orders_by_day(cfg))
    e = _Emitter(
        {"world_id": "world-" + "0" * 32, "orders": orders}, "dataset-" + "0" * 32, config.start
    )
    _emit_sources(
        e,
        datetime.fromisoformat(config.start.replace("Z", "+00:00")),
        cfg,
        catalog=[f"sku-{i + 1:05d}" for i in range(config.sku_count)],
        finalize=True,
        listing_corrections=False,
        inventory_day=config.followup_days - 4,
    )
    fixed = [
        r
        for f, records in e.rows.items()
        for r in records
        if f in ("seller_wms.inbound_events", "seller_wms.stock_snapshots")
        or r["payload"].get("operation_type") in ("opening", "inbound_receipt")
    ]
    fixed_ids = {r["source_event_id"] for r in fixed}
    rows = [
        r
        for f, records in e.rows.items()
        if f != "seller_catalog.listing_versions"
        for r in records
        if r["source_event_id"] not in fixed_ids
    ]
    native = [_native(o) for o in orders]
    return {
        "rows": len(rows) + len(e.uncertainties) + count,
        "bytes": sum(len(canonical(r).encode()) + 1 for r in rows + native + e.uncertainties),
        "fixed_rows": len(fixed),
        "fixed_bytes": sum(len(canonical(r).encode()) + 1 for r in fixed),
    }


class _Files:
    """Append by export_date with at most 24 handles and incremental hashing."""

    def __init__(self, root, dataset):
        """Initialize the file registry without holding content in RAM."""
        self.root, self.dataset = root, dataset
        self.handles, self.entries, self.hashes = OrderedDict(), {}, {}

    def write(self, feed, producer, row, date):
        """Write one canonical record into the right directory partition."""
        rel = f"dataset_version={self.dataset}/producer={producer}/feed={feed}/export_date={date}/part-00000.jsonl"
        if rel not in self.entries:
            path = self.root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            self.entries[rel] = {
                "path": rel,
                "role": "source",
                "feed": feed,
                "producer": producer,
                "rows": 0,
                "bytes": 0,
            }
            self.hashes[rel] = hashlib.sha256()
        if rel not in self.handles:
            if len(self.handles) >= 24:
                self.handles.popitem(last=False)[1].close()
            self.handles[rel] = (self.root / rel).open("ab")
        handle = self.handles.pop(rel)
        self.handles[rel] = handle
        raw = (canonical(row) + "\n").encode()
        handle.write(raw)
        entry = self.entries[rel]
        entry["rows"] += 1
        entry["bytes"] += len(raw)
        self.hashes[rel].update(raw)

    def finish(self):
        """Close every handle and return metadata with finalized hashes."""
        for handle in self.handles.values():
            handle.close()
        self.handles.clear()
        return [
            entry | {"sha256": self.hashes[rel].hexdigest()}
            for rel, entry in sorted(self.entries.items())
        ]


def _prepare_day(e, orders, config, start, first=False, last=False):
    """Use the shared renderer, pass the observation delay and preserve causal known-time."""
    e.world["orders"] = orders
    _emit_sources(
        e,
        start,
        config,
        catalog=[f"sku-{i + 1:05d}" for i in range(config.sku_count)],
        opening_quantity=config.days * config.orders_per_day * 12 + 200,
        initialize=first,
        finalize=last,
        listing_corrections=False,
        inventory_day=config.followup_days - 4,
    )
    late = {
        o["native_order_id"]: {line["native_line_id"]: line["late_seconds"] for line in o["lines"]}
        for o in orders
    }
    rows = [r for records in e.rows.values() for r in records]
    refs = {r["source_event_id"]: r for r in rows}
    refs.update({r["source_event_id"]: r for r in e.openings.values()})
    for row in e.rows["carrier.delivery_events"]:
        delay = late[row["payload"]["native_order_id"]][row["payload"]["native_line_id"]]
        if delay:
            row["source_observed_at"] = _stamp(
                datetime.fromisoformat(row["effective_at"].replace("Z", "+00:00"))
                + timedelta(seconds=delay)
            )
    for _ in range(12):
        changed = False
        for row in rows:
            known = max(
                [row["source_observed_at"]]
                + [refs[r]["source_observed_at"] for r in row["depends_on_event_ids"]]
            )
            if known != row["source_observed_at"]:
                row["source_observed_at"] = known
                changed = True
        if not changed:
            break
    else:
        raise ValueError("causal propagation did not converge")
    for u in e.uncertainties:
        u["affected_scope"]["candidate_native_line_ids"] = [
            line["native_line_id"]
            for o in orders
            if o["native_order_id"] == u["affected_scope"]["native_order_id"]
            for line in o["lines"]
        ]
        ref = u.get("resolution_event_ref", u["source_event_ref"])
        u["source_observed_at"] = max(u["source_observed_at"], refs[ref]["source_observed_at"])
    return rows


def _native_scaled(order, version, index):
    """Reuse the native renderer and fixture-like overrides, updating totals per line."""
    channel = order["channel"]
    row = _native(order, version if channel == "amazon" else 1)
    total = f"{len(order['lines']) * 300}.00"
    if channel == "amazon":
        (row["proceeds"]["grandTotal"] if version == 2 else row["OrderTotal"])[
            "amount" if version == 2 else "Amount"
        ] = total
    elif channel == "shopify":
        row["order"]["total_price"] = total
    elif channel == "shopee" and version == 2:
        body = row["response"]["order_list"][0]
        body["order_number"] = body.pop("order_sn")
        for item in body["item_list"]:
            item["sku"] = item.pop("model_sku")
    elif channel == "tiktok_shop" and version == 2 and index % 2 == 0:
        row["data"]["orders"][0]["buyer_message"] = "Please pack carefully"
    return row


def _array_hash(strings):
    """Hash a canonical JSON array through a sorted iterator without collecting the whole set."""
    h = hashlib.sha256(b"[")
    for index, raw in enumerate(strings):
        if index:
            h.update(b",")
        h.update(raw.encode())
    h.update(b"]")
    return h.hexdigest()


def _scale_identity(config, parameters):
    """The first pass only hashes the world day by day; independent of renderer schema and machine clock.

    Return (physical_sha, intent_sha, world_id, identity, dataset). `dataset` is the hash of
    `identity`, which includes the generator code hash: changing the code yields a new dataset_version.
    """
    physical = hashlib.sha256()
    intents = hashlib.sha256()
    for orders in _orders_by_day(config):
        for order in orders:
            physical.update((canonical(order) + "\n").encode())
            for line in order["lines"]:
                intents.update(canonical([line["physical_line_id"], line["intent_unit"]]).encode())
    physical_sha = physical.hexdigest()
    world_id = stable_id("world", physical_sha)
    identity = {
        "configuration": parameters,
        "world_sha256": physical_sha,
        "contract_sha256": hashlib.sha256(
            (
                Path(__file__).parents[1]
                / "docs/data-platform/reference/source_contract_order_lifecycle.md"
            ).read_bytes()
        ).hexdigest(),
        "source_profiles": PROFILES,
        "fact_hash_policy": "canonical envelope excluding recorded_at; coverage full rows",
        "generator_sha256": digest(
            [
                hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                hashlib.sha256(
                    Path(__file__).with_name("generate_order_lifecycle_fixture.py").read_bytes()
                ).hexdigest(),
            ]
        ),
        "runtime": {"python": platform.python_version()},
        "schema_version": "seller-source-v2/1",
        "module_versions": {
            "world": "sampled-physical-world-v2/1",
            "renderer": "shared-source-v2-scale/1",
        },
        "renderer_configuration": {"native_schemas": dict(config.native_schemas)},
    }
    return physical_sha, intents.hexdigest(), world_id, identity, stable_id("dataset", identity)


def _open_spool(spool):
    """Temporary SQLite holding every event on disk to re-sort by known-time without keeping it in RAM."""
    db = sqlite3.connect(str(Path(spool) / "events.sqlite"))
    db.execute("PRAGMA journal_mode=OFF")
    db.execute("PRAGMA synchronous=OFF")
    db.execute("PRAGMA temp_store=FILE")
    db.execute("PRAGMA cache_size=-8192")
    db.execute(
        "CREATE TABLE events (feed TEXT, producer TEXT, observed TEXT, eid TEXT, raw TEXT, effective TEXT, seq INTEGER)"
    )
    return db


def _spool_day(
    e,
    db,
    writer,
    truth,
    orders,
    day,
    config,
    start,
    horizons,
    world_id,
    recorded,
    counts,
    native_ids,
):
    """One order day: events go to the spool, uncertainty + native orders are written directly, the oracle goes to truth.

    Return (the day's cohort, the day's largest observation delay in seconds).
    """
    max_delay = 0
    rows = _prepare_day(e, orders, config, start, first=day == 0, last=day == config.days - 1)
    for row in rows:
        if (
            not horizons["order_creation_start"]
            <= row["effective_at"]
            < horizons["lifecycle_followup_end"]
            or not row["effective_at"]
            <= row["source_observed_at"]
            < horizons["source_observation_end"]
        ):
            raise ValueError("source event overflow")
        max_delay = max(
            max_delay,
            int(
                (
                    datetime.fromisoformat(row["source_observed_at"].replace("Z", "+00:00"))
                    - datetime.fromisoformat(row["effective_at"].replace("Z", "+00:00"))
                ).total_seconds()
            ),
        )
        db.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?,0)",
            (
                row["feed"],
                row["producer"],
                row["source_observed_at"],
                row["source_event_id"],
                canonical(row),
                row["effective_at"],
            ),
        )
    for u in e.uncertainties:
        u.update(
            schema_id="seller-source-v2/source.uncertainties/1",
            world_id=world_id,
            recorded_at=recorded,
        )
        writer.write("source.uncertainties", u["producer"], u, u["source_observed_at"][:10])
    day_open, day_lines = 0, 0
    for index, order in enumerate(orders):
        channel = order["channel"]
        version = dict(config.native_schemas).get(channel, 1)
        writer.write(
            channel + ".orders",
            "sim.native." + channel,
            _native_scaled(order, version, index),
            order["created_at"][:10],
        )
        native_ids[channel].append(order["native_order_id"])
        counts["orders"] += 1
        for line in order["lines"]:
            scenario = line["scenario"]
            counts[scenario] += 1
            counts["lines"] += 1
            day_lines += 1
            day_open += line["open"]
            key = stable_id(
                "line",
                e.dataset,
                "seller-1",
                channel,
                order["account_id"],
                order["native_order_id"],
                {"native_line_id": line["native_line_id"]},
            )
            oracle = {
                "line_id": key,
                "physical_line_id": line["physical_line_id"],
                "scenario": scenario,
                "sku": line["sku"],
                "ordered": 3,
                "canceled": line["canceled"],
                "delivered": 0 if line["open"] else 3 - line["canceled"],
                "returned": int(line["return"]),
                "approved_at": line["schedule"]["approved_at"] if line["return"] else None,
                "refund_success_at": line["schedule"]["approved_at"] if line["refund"] else None,
            }
            truth.write(canonical(oracle) + "\n")
    cohort = {
        "order_date": _stamp(start + timedelta(days=day))[:10],
        "orders": len(orders),
        "terminal_lines": day_lines - day_open,
        "right_censored_lines": day_open,
    }
    db.commit()
    e.rows.clear()
    e.uncertainties.clear()
    e.world["orders"] = []
    return cohort, max_delay


def _write_sequenced_events(db, writer):
    """Re-sort by known-time on disk; the producer sequence is contiguous and global."""
    db.execute("CREATE INDEX observation_order ON events(producer,observed)")
    sequences = Counter()
    for rowid, raw in db.execute("SELECT rowid,raw FROM events ORDER BY producer,observed,rowid"):
        row = json.loads(raw)
        producer = row["producer"]
        sequences[producer] += 1
        row["source_sequence"] = sequences[producer]
        raw = canonical(row)
        db.execute(
            "UPDATE events SET raw=?,seq=? WHERE rowid=?", (raw, row["source_sequence"], rowid)
        )
        writer.write(row["feed"], producer, row, row["source_observed_at"][:10])
    db.commit()


def _week_coverage(
    db, feed, producer, profile, capability, scope, week, config, start, dataset, world_id, recorded
):
    """Coverage `complete` for one feed over one event week `[begin, end)` (ADR-0013)."""
    begin = _stamp(start + timedelta(days=week))
    end = _stamp(start + timedelta(days=min(week + 7, config.followup_days)))
    params = (feed, begin, end)
    last_observed = db.execute(
        "SELECT max(observed) FROM events WHERE feed=? AND effective>=? AND effective<?", params
    ).fetchone()[0]
    # Close at least 1 day after the week ends, or 1µs after the last late row.
    observed = datetime.fromisoformat(end.replace("Z", "+00:00")) + timedelta(days=1)
    if last_observed:
        observed = max(
            observed,
            datetime.fromisoformat(last_observed.replace("Z", "+00:00"))
            + timedelta(microseconds=1),
        )
    ranges = []
    for (seq,) in db.execute(
        "SELECT seq FROM events WHERE feed=? AND effective>=? AND effective<? ORDER BY seq", params
    ):
        if ranges and ranges[-1]["last"] + 1 == seq:
            ranges[-1]["last"] = seq
        else:
            ranges.append({"partition": "partition-0", "first": seq, "last": seq})
    return {
        "schema_id": "seller-source-v2/source.coverage_envelopes/1",
        "world_id": world_id,
        "recorded_at": recorded,
        "coverage_id": stable_id("coverage", dataset, feed, week),
        "producer": producer,
        "feed": feed,
        "dataset_version": dataset,
        "source_epoch": producer + ":" + dataset,
        "profile_id": profile,
        "scope": scope,
        "capability": capability,
        "event_interval": {"start_inclusive": begin, "end_exclusive": end},
        "status": "complete",
        "expected_partitions": ["partition-0"],
        "completed_partitions": ["partition-0"],
        "source_sequence_ranges": ranges,
        "source_record_count": db.execute(
            "SELECT count(*) FROM events WHERE feed=? AND effective>=? AND effective<?", params
        ).fetchone()[0],
        "record_set_sha256": _array_hash(
            raw
            for (raw,) in db.execute(
                "SELECT raw FROM events WHERE feed=? AND effective>=? AND effective<? ORDER BY eid",
                params,
            )
        ),
        "unresolved_scope_refs": [],
        "source_observed_at": _stamp(observed),
        "clock_domain": CLOCK,
        "revision": 1,
    }


def _write_coverage(db, writer, config, start, horizons, dataset, world_id, recorded):
    """Write weekly coverage for every business feed; return the authority list for the manifest.

    Feeds whose pool is Channel-managed have no operational authority, so they get an `unavailable` coverage.
    """
    db.execute("CREATE INDEX feed_interval ON events(feed,effective)")
    authority = []
    for feed, (short, capability, profile) in FEEDS.items():
        producer = "sim.seller." + short
        scope = {
            "seller_id": "seller-1",
            "account_ids": [f"account-{c}-1" for c in CHANNELS],
            "channels": list(CHANNELS),
            "pool_ids": ["seller-shared-1"],
        }
        authority.append(
            {
                "authority_id": stable_id("authority", dataset, feed),
                "producer": producer,
                "feed": feed,
                "capability": capability,
                "profile_id": profile,
                "status": "available",
                "scope": scope,
                "valid_from": horizons["order_creation_start"],
                "valid_to": horizons["source_observation_end"],
            }
        )
        # Each row belongs to exactly one week; the on-disk index keeps total hashing linear.
        for week in range(0, config.followup_days, 7):
            cov = _week_coverage(
                db,
                feed,
                producer,
                profile,
                capability,
                scope,
                week,
                config,
                start,
                dataset,
                world_id,
                recorded,
            )
            writer.write("source.coverage_envelopes", producer, cov, cov["source_observed_at"][:10])
        if feed in (
            "carrier.delivery_events",
            "seller_postsales.return_events",
            "seller_postsales.refund_events",
            "seller_wms.stock_operations",
            "seller_wms.inbound_events",
            "seller_wms.stock_snapshots",
        ):
            unavailable_scope = {
                "seller_id": "seller-1",
                "account_ids": ["channel-managed-account"],
                "channels": ["amazon"],
                "pool_ids": ["channel-managed-1"],
            }
            authority.append(
                {
                    **authority[-1],
                    "authority_id": stable_id("authority-unavailable", dataset, feed),
                    "profile_id": "channel-orders-limited/1",
                    "status": "unavailable",
                    "scope": unavailable_scope,
                    "reason": "channel-managed pool has no simulated operational authority",
                }
            )
            writer.write(
                "source.coverage_envelopes",
                producer,
                {
                    **cov,
                    "coverage_id": stable_id("coverage-unavailable", dataset, feed),
                    "scope": unavailable_scope,
                    "profile_id": "channel-orders-limited/1",
                    "status": "unavailable",
                    "capability_status": "unavailable",
                    "expected_partitions": [],
                    "completed_partitions": [],
                    "source_sequence_ranges": [],
                    "source_record_count": 0,
                    "record_set_sha256": digest([]),
                },
                cov["source_observed_at"][:10],
            )
    return authority


def _add_empty_feed_files(root, dataset, horizons, files):
    """Empty feeds still need a file for profile/consumer discovery."""
    for feed in list(FEEDS) + ["source.uncertainties"] + [c + ".orders" for c in CHANNELS]:
        if not any(f["feed"] == feed for f in files):
            producer = (
                "sim.seller." + FEEDS[feed][0]
                if feed in FEEDS
                else (
                    "sim.native." + feed[:-7]
                    if feed.endswith(".orders")
                    else "sim.seller.postsales"
                )
            )
            rel = f"dataset_version={dataset}/producer={producer}/feed={feed}/export_date={horizons['order_creation_start'][:10]}/part-00000.jsonl"
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"")
            files.append(
                {
                    "path": rel,
                    "role": "source",
                    "feed": feed,
                    "producer": producer,
                    "rows": 0,
                    "bytes": 0,
                    "sha256": hashlib.sha256(b"").hexdigest(),
                }
            )
    files.sort(key=lambda f: f["path"])


def _native_sources(files, native_ids, config, horizons):
    """Describe each Channel's native snapshot for the manifest: Walmart via JDBC, the rest via files."""
    native_sources = {}
    for channel in CHANNELS:
        channel_files = [f for f in files if f["feed"] == channel + ".orders"]
        native_sources[channel] = {
            "transport": "postgres_bootstrap_only" if channel == "walmart" else "object_store_file",
            "ingestion_boundary": "jdbc" if channel == "walmart" else "file",
            "snapshot_mode": "full_entity",
            "snapshot_scope": {"order_ids": native_ids[channel]},
            "snapshot_as_of": horizons["order_creation_end"],
            "page_set_complete": True,
            "schema_id": f"sim-native-v2/{channel}/{dict(config.native_schemas).get(channel, 1)}",
            "file_set_digest": channel_files[0]["sha256"]
            if len(channel_files) == 1
            else digest([f["sha256"] for f in channel_files]),
            "source_observed_at": horizons["order_creation_end"],
            "account_id": f"account-{channel}-1",
        }
    return native_sources


def _file_sha256(path):
    """SHA-256 of a file, read one MiB at a time to avoid holding the whole file in RAM."""
    sha = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def generate_scaled(output_root, config: ScaleConfig):
    """Export the scaled source set into a new directory, truth kept separate, no services or network.

    Steps: hash the world → push each day's events into the SQLite spool → re-sort by known-time
    and assign sequence → weekly coverage → empty files for empty feeds → manifest.
    """
    began = time.perf_counter()
    start = config.validate()
    if "://" in str(output_root):
        raise ValueError("scale requires a local output root")
    root = Path(output_root)
    if root.exists():
        raise FileExistsError(root)
    parameters = _params(config)
    physical_sha, intent_sha, world_id, identity, dataset = _scale_identity(config, parameters)
    horizons = {
        "clock_domain": CLOCK,
        "order_creation_start": _stamp(start),
        "order_creation_end": _stamp(start + timedelta(days=config.days)),
        "lifecycle_followup_end": _stamp(start + timedelta(days=config.followup_days)),
        "source_observation_end": _stamp(start + timedelta(days=config.observation_days)),
    }
    # Frozen clock of the source export; real run time only appears in the manifest.
    recorded = horizons["source_observation_end"]
    root.mkdir(parents=True, exist_ok=False)
    writer = _Files(root, dataset)
    truth_path = root / "_truth/lines.jsonl"
    truth_path.parent.mkdir()
    counts, cohorts, native_ids = Counter(), [], defaultdict(list)
    max_delay = 0
    e = _Emitter({"world_id": world_id, "orders": []}, dataset, recorded)
    try:
        with (
            tempfile.TemporaryDirectory(prefix=".scale-spool-", dir=root) as spool,
            _open_spool(spool) as db,
            truth_path.open("w") as truth,
        ):
            for day, orders in enumerate(_orders_by_day(config)):
                cohort, day_delay = _spool_day(
                    e,
                    db,
                    writer,
                    truth,
                    orders,
                    day,
                    config,
                    start,
                    horizons,
                    world_id,
                    recorded,
                    counts,
                    native_ids,
                )
                cohorts.append(cohort)
                max_delay = max(max_delay, day_delay)
                del orders
            _write_sequenced_events(db, writer)
            authority = _write_coverage(
                db, writer, config, start, horizons, dataset, world_id, recorded
            )
    finally:
        files = writer.finish()
    _add_empty_feed_files(root, dataset, horizons, files)
    native_sources = _native_sources(files, native_ids, config, horizons)
    total_rows = sum(f["rows"] for f in files)
    if total_rows > MAX_SOURCE_ROWS:
        raise ValueError("actual source records exceed ceiling")
    pinned = {
        "dataset_version": dataset,
        "world_id": world_id,
        "identity": identity,
        "horizons": horizons,
    }
    (root / "identity.json").write_text(json.dumps(pinned, ensure_ascii=False, indent=2) + "\n")
    manifest = {
        "contract_id": "seller-source-v2",
        "dataset_version": dataset,
        "world_id": world_id,
        **identity,
        "physical_outcome_sha256": physical_sha,
        "intent_sha256": intent_sha,
        "horizons": horizons,
        "authority_capabilities": authority,
        "native_sources": native_sources,
        "files": files,
        "counts": {"orders": counts["orders"], "skus": config.sku_count, "lines": counts["lines"]},
        "recorded_at": _stamp(datetime.now(timezone.utc)),
        "execution_clock_domain": "execution_utc",
        "source_recorded_at_policy": "frozen export timestamp = source_observation_end, distinct from execution recorded_at",
        "tail_diagnostics": {
            "right_censored_lines": counts["right_censored"],
            "terminal_lines": counts["lines"] - counts["right_censored"],
            "maturity_deadline_passed_lines": counts["lines"] - counts["right_censored"],
            "bytes": sum(
                f["bytes"]
                for f in files
                if f["producer"].startswith("sim.seller.")
                and f["path"].split("export_date=")[1][:10] >= horizons["order_creation_end"][:10]
            ),
            "bounds": {
                "max_fulfillment_lag_days": 4,
                "qualification_days_inclusive": 30,
                "followup_allowance_days": 15,
                "max_source_delay_seconds": max_delay,
            },
            "cohorts": cohorts,
            "reason": "right_censored sampled explicitly; maturity counts are not official label eligibility",
        },
        "runtime_seconds": round(time.perf_counter() - began, 6),
        "source_bytes": sum(f["bytes"] for f in files),
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "currency_table": {"version": "fixture-currency/1", "USD": {"minor_unit_scale": 2}},
        "native_snapshot_policy": "frozen creation observations; no native status refresh or actual-delivery claim",
        "scenario_distribution": {
            "type": "seeded_sampled_scale",
            "random_sampling": True,
            "seed": config.seed,
            "parameters": parameters,
            "weights": SCENARIO_WEIGHTS,
            "counts": {s: counts[s] for s in SCENARIO_WEIGHTS},
            "actual_rates": {s: counts[s] / counts["lines"] for s in SCENARIO_WEIGHTS},
        },
        "truth": {
            "path": "_truth/lines.jsonl",
            "not_a_source": True,
            "rows": counts["lines"],
            "sha256": _file_sha256(truth_path),
        },
        "limits": [
            "local source export only; no Bronze/Silver/labels/publication claim",
            "Walmart bootstrap is not JDBC proof",
            "20 million worst-case source rows; disk spool required",
        ],
    }
    (root / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    return manifest


def main():
    """CLI that prints the estimate before generating and only writes to a new directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--orders-per-day", type=int, required=True)
    parser.add_argument("--days", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--start", default=ScaleConfig.start)
    parser.add_argument("--sku-count", type=int, default=ScaleConfig.sku_count)
    parser.add_argument("--sku-zipf-s", type=float, default=ScaleConfig.sku_zipf_s)
    parser.add_argument("--channel-weights", default=",".join(c + "=1" for c in CHANNELS))
    parser.add_argument("--native-schema-for", action="append", default=[])
    parser.add_argument("--followup-days", type=int, default=ScaleConfig.followup_days)
    parser.add_argument("--observation-days", type=int, default=ScaleConfig.observation_days)
    parser.add_argument("--sku-return-skew", type=float, default=ScaleConfig.sku_return_skew)
    args = parser.parse_args()
    try:
        weights = dict(
            (c, float(v)) for c, v in (p.split("=", 1) for p in args.channel_weights.split(","))
        )
        if set(weights) != set(CHANNELS):
            raise ValueError("channel-weights must name all five channels")
        overrides = tuple((c, int(v)) for c, v in (p.split("=", 1) for p in args.native_schema_for))
        config = ScaleConfig(
            seed=args.seed,
            start=args.start,
            days=args.days,
            orders_per_day=args.orders_per_day,
            sku_count=args.sku_count,
            sku_zipf_s=args.sku_zipf_s,
            channel_weights=tuple(weights[c] for c in CHANNELS),
            native_schemas=overrides,
            followup_days=args.followup_days,
            observation_days=args.observation_days,
            sku_return_skew=args.sku_return_skew,
        )
        print(json.dumps({"estimate": config.estimate()}, ensure_ascii=False), flush=True)
        manifest = generate_scaled(args.output, config)
    except (ValueError, FileExistsError) as exc:
        parser.error(str(exc))
    print(
        json.dumps(
            {
                k: manifest[k]
                for k in (
                    "dataset_version",
                    "counts",
                    "source_bytes",
                    "runtime_seconds",
                    "peak_rss_kib",
                )
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
