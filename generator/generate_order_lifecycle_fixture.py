"""Small seller-source-v2 source fixture, local only, independent of the v1 Batch engine.

Run `python -m generator.generate_order_lifecycle_fixture --output /new/local/root`. The module
only exports sources: it does not publish labels, load PostgreSQL, or run the Data Platform pipeline.
The Walmart JSONL is only for building the DB, not a replacement for the JDBC load path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

CHANNELS = ("amazon", "walmart", "shopify", "tiktok_shop", "shopee")
CLOCK = "simulation_business_utc:v2"
FEEDS = {
    "seller_oms.line_commitments": (
        "oms",
        "ordered_quantity_money_cancellation",
        "seller-operated-label-v2/1",
    ),
    "seller_oms.fulfillment_allocations": (
        "oms",
        "unit_allocation_binding",
        "seller-operated-label-v2/1",
    ),
    "carrier.delivery_events": (
        "carrier",
        "actual_delivery_with_allocation",
        "seller-operated-label-v2/1",
    ),
    "seller_postsales.return_events": (
        "postsales",
        "approved_return_with_allocation",
        "seller-operated-label-v2/1",
    ),
    "seller_postsales.refund_events": (
        "postsales",
        "product_refund_success_with_allocation",
        "seller-operated-label-v2/1",
    ),
    "seller_catalog.listing_versions": (
        "catalog",
        "listing_mapping_history",
        "seller-stock-triage/1",
    ),
    "seller_wms.stock_operations": ("wms", "stock_operation_ledger", "seller-stock-triage/1"),
    "seller_wms.inbound_events": ("wms", "inbound_receipt", "seller-stock-triage/1"),
    "seller_wms.stock_snapshots": ("wms", "stock_snapshot", "seller-stock-triage/1"),
}

PROFILES = {
    "core-orders-five-channel/1": {"required_feeds": [c + ".orders" for c in CHANNELS]},
    "seller-operated-label-v2/1": {
        "required_feeds": [
            "seller_oms.line_commitments",
            "seller_oms.fulfillment_allocations",
            "carrier.delivery_events",
            "seller_postsales.return_events",
            "seller_postsales.refund_events",
            "source.coverage_envelopes",
            "source.uncertainties",
        ]
    },
    "seller-stock-triage/1": {
        "required_feeds": [
            "seller_oms.line_commitments",
            "seller_oms.fulfillment_allocations",
            "seller_catalog.listing_versions",
            "seller_wms.stock_operations",
            "seller_wms.inbound_events",
            "source.coverage_envelopes",
        ],
        "optional_feeds": ["seller_wms.stock_snapshots"],
    },
    "channel-orders-limited/1": {
        "required_feeds": ["amazon.orders", "seller_catalog.listing_versions"],
        "unavailable_capabilities": ["actual_delivery", "postsales", "stock"],
    },
}


def canonical(value):
    """Canonical UTF-8 JSON, used for identities and receipts of the emitted set."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def digest(value):
    """SHA-256 (hex) of a value's canonical JSON form."""
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def stable_id(kind, *parts):
    """Stable id from a versioned tuple; distinguishes separators, missing keys and null."""
    return kind + "-" + digest(["seller-source-v2/identity/1", kind, *parts])[:32]


@dataclass(frozen=True)
class FixtureConfig:
    """Fixture config; hard limits so this API cannot accidentally generate 100 GB."""

    seed: int = 7
    start: str = "2026-01-01T00:00:00.000000Z"
    creation_days: int = 3
    followup_days: int = 90
    observation_days: int = 105
    approval_lag_days: int = 29

    def validate(self):
        """Reject configs outside the fixture contract; return the UTC start time."""
        if self.approval_lag_days not in (29, 31):
            raise ValueError("fixture approval_lag_days must be 29 or 31")
        if not 2 <= self.creation_days <= 7:
            raise ValueError("fixture creation_days must be 2..7")
        if not self.creation_days < self.followup_days < self.observation_days <= 120:
            raise ValueError("three horizons must be strictly increasing, at most 120 days")
        if (
            self.followup_days < self.creation_days + 50
            or self.observation_days < self.followup_days + 9
        ):
            raise ValueError(
                "fixture tail must cover delivery, 30d qualification, allowance and late corrections/coverage"
            )
        start = datetime.fromisoformat(self.start.replace("Z", "+00:00"))
        if start.utcoffset() != timedelta(0):
            raise ValueError("start must be UTC")
        return start


def _stamp(value):
    """Format a datetime as ISO-8601 UTC with microseconds and a Z suffix."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _world(config):
    """Build the deterministic physical world: ids, orders, lines shared by every Channel."""
    start = config.validate()
    world_config = {
        "seed": config.seed,
        "start": _stamp(start),
        "creation_days": config.creation_days,
        "semantics": "physical-world-v2/1",
        "catalog": ["sku-A", "sku-B"],
        "buyer_population": "one-channel-local-buyer-per-order/1",
    }
    population_id = stable_id("population", world_config)
    world_config["approval_lag_days"] = config.approval_lag_days
    world_id = stable_id("world", world_config)
    orders = []
    for day in range(config.creation_days):
        for channel in CHANNELS:
            created = start + timedelta(days=day, hours=10)
            order_id = stable_id("native-order", population_id, channel, day)
            lines = []
            for index, sku in enumerate(world_config["catalog"]):
                line_id = stable_id("physical-line", population_id, channel, day, index)
                lines.append(
                    {
                        "physical_line_id": line_id,
                        "native_line_id": str(index + 1),
                        "sku": sku,
                        "quantity": 3,
                        "canceled": 1 if index == 0 else 0,
                        "units": [stable_id("unit", line_id, i) for i in range(3)],
                        "intent_unit": stable_id("unit", line_id, 0) if index == 0 else None,
                        "ordered_net_minor": 30000,
                        "unit_gross_minor": 11000,
                        "unit_discount_minor": 1000,
                    }
                )
            for index, line in enumerate(lines):
                groups = (
                    [line["units"][:2]] if index == 0 else [line["units"][:2], line["units"][2:]]
                )
                line["delivery_plan"] = [
                    {
                        "physical_unit_ids": group,
                        "ship_at": _stamp(created + timedelta(days=1 + part)),
                        "delivered_at": _stamp(created + timedelta(days=3 + part)),
                    }
                    for part, group in enumerate(groups)
                ]
            orders.append(
                {
                    "channel": channel,
                    "account_id": f"account-{channel}-1",
                    "native_order_id": order_id,
                    "created_at": _stamp(created),
                    "ship_at": _stamp(created + timedelta(days=1)),
                    "delivered_at": _stamp(created + timedelta(days=3)),
                    "approved_at": _stamp(created + timedelta(days=3 + config.approval_lag_days)),
                    "lines": lines,
                }
            )
    return {"world_id": world_id, "configuration": world_config, "orders": orders}


def _native(order, schema=1):
    """Small native-shaped snapshot; money is only integers or decimal strings.

    The fixture's schema ids deliberately differ from the v1 renderer; operational truth never leaks into native payloads.
    """
    oid, channel = order["native_order_id"], order["channel"]
    lines = order["lines"]
    if channel == "amazon":
        if schema == 2:
            return {
                "orderId": oid,
                "createdTime": order["created_at"],
                "fulfillment": {"status": "UNSHIPPED"},
                "proceeds": {"grandTotal": {"currencyCode": "USD", "amount": "600.00"}},
                "orderItems": [
                    {
                        "orderItemId": line["native_line_id"],
                        "sellerSku": line["sku"],
                        "quantityOrdered": 3,
                        "price": {"currencyCode": "USD", "amount": "330.00"},
                        "discount": {"currencyCode": "USD", "amount": "30.00"},
                    }
                    for line in lines
                ],
            }
        return {
            "AmazonOrderId": oid,
            "PurchaseDate": order["created_at"],
            "OrderStatus": "Unshipped",
            "OrderTotal": {"CurrencyCode": "USD", "Amount": "600.00"},
            "OrderItems": [
                {
                    "OrderItemId": line["native_line_id"],
                    "SellerSKU": line["sku"],
                    "QuantityOrdered": 3,
                    "ItemPrice": {"CurrencyCode": "USD", "Amount": "330.00"},
                    "PromotionDiscount": {"CurrencyCode": "USD", "Amount": "30.00"},
                }
                for line in lines
            ],
        }
    if channel == "walmart":
        payload = {
            "purchaseOrderId": oid,
            "orderDate": int(
                datetime.fromisoformat(order["created_at"].replace("Z", "+00:00")).timestamp()
                * 1000
            ),
            "orderLines": {
                "orderLine": [
                    {
                        "lineNumber": line["native_line_id"],
                        "item": {"sku": line["sku"]},
                        "orderLineQuantity": {"unitOfMeasurement": "EACH", "amount": "3"},
                        "charges": {
                            "charge": [
                                {
                                    "chargeType": "PRODUCT",
                                    "chargeAmount": {"currency": "USD", "amount": "300.00"},
                                }
                            ]
                        },
                    }
                    for line in lines
                ]
            },
        }
        return {"purchase_order_id": oid, "raw_payload": payload, "fetched_at": order["created_at"]}
    if channel == "shopify":
        return {
            "order": {
                "id": oid,
                "created_at": order["created_at"],
                "currency": "USD",
                "total_price": "600.00",
                "line_items": [
                    {
                        "id": line["native_line_id"],
                        "sku": line["sku"],
                        "quantity": 3,
                        "current_quantity": 3,
                        "price": "110.00",
                        "total_discount": "30.00",
                    }
                    for line in lines
                ],
            }
        }
    if channel == "tiktok_shop":
        return {
            "data": {
                "orders": [
                    {
                        "id": oid,
                        "status": "AWAITING_SHIPMENT",
                        "currency": "USD",
                        "line_items": [
                            {
                                "id": line["native_line_id"],
                                "seller_sku": line["sku"],
                                "quantity": 3,
                                "original_price": "110.00",
                                "sale_price": "100.00",
                                "seller_discount": "10.00",
                            }
                            for line in lines
                        ],
                    }
                ]
            }
        }
    return {
        "response": {
            "order_list": [
                {
                    "order_sn": oid,
                    "currency": "USD",
                    "order_status": "READY_TO_SHIP",
                    "item_list": [
                        {
                            "order_item_id": line["native_line_id"],
                            "model_sku": line["sku"],
                            "model_quantity_purchased": 3,
                            "model_original_price": "110.00",
                            "model_discounted_price": "100.00",
                        }
                        for line in lines
                    ],
                }
            ]
        }
    }


class _Emitter:
    """Group source rows by feed, assigning per-producer sequences and stable event ids."""

    def __init__(self, world, dataset, recorded_at):
        """Initialize empty feeds for one dataset version of one world."""
        self.world = world
        self.dataset = dataset
        self.recorded_at = recorded_at
        self.rows = defaultdict(list)
        self.sequences = defaultdict(int)
        self.uncertainties = []

    def add(
        self,
        feed,
        entity,
        payload,
        effective,
        observed=None,
        revision=1,
        operation="lifecycle_event",
        corrects=None,
        depends=(),
    ):
        """Append a source row to a feed and return that row."""
        producer = "sim.seller." + FEEDS[feed][0]
        self.sequences[producer] += 1
        eid = stable_id(
            "event",
            self.dataset,
            feed,
            entity,
            revision,
            payload.get("event_type"),
            payload.get("allocation_state"),
        )
        row = {
            "schema_id": f"seller-source-v2/{feed}/1",
            "dataset_version": self.dataset,
            "world_id": self.world["world_id"],
            "source_epoch": producer + ":" + self.dataset,
            "producer": producer,
            "feed": feed,
            "source_event_id": eid,
            "entity_id": entity,
            "entity_revision": revision,
            "source_sequence": self.sequences[producer],
            "source_partition": "partition-0",
            "operation": operation,
            "corrects_source_event_id": corrects,
            "depends_on_event_ids": list(depends),
            "effective_at": effective,
            "source_observed_at": observed or effective,
            "clock_domain": CLOCK,
            "recorded_at": self.recorded_at,
            "payload": payload,
        }
        self.rows[feed].append(row)
        return row


def _shift(stamp, days):
    """ISO timestamp `...Z` plus `days` days, returned in contract form."""
    return _stamp(datetime.fromisoformat(stamp.replace("Z", "+00:00")) + timedelta(days=days))


class _StockChain:
    """Inventory operation chain of ONE line: each operation depends on the previous one, deltas add to the SKU balance."""

    def __init__(self, e, line_key, base, opening, balance):
        """Start the chain from the SKU's opening operation."""
        self.e, self.line_key, self.base, self.balance = e, line_key, base, balance
        self.previous = opening

    def add(self, kind, qty, dh, dr, dn, effective, extra=None):
        """Emit one inventory operation and add its delta to the current balance."""
        opid = stable_id("stock-op", self.line_key, kind)
        p = {
            **self.base,
            "operation_id": opid,
            "operation_type": kind,
            "quantity": qty,
            "depends_on_operation_ids": [self.previous["payload"]["operation_id"]],
            "delta_on_hand": dh,
            "delta_reserved": dr,
            "delta_non_sellable": dn,
            **(extra or {}),
        }
        self.previous = self.e.add(
            "seller_wms.stock_operations",
            opid,
            p,
            effective,
            depends=[self.previous["source_event_id"]],
        )
        for name, delta in (("on_hand", dh), ("reserved", dr), ("non_sellable", dn)):
            self.balance[name] += delta
        return self.previous


def _emit_sources(
    e,
    start,
    config,
    delivery_parts=1,
    *,
    catalog=None,
    opening_quantity=200,
    initialize=True,
    finalize=True,
    listing_corrections=True,
    inventory_day=45,
):
    """Emit the shared sources; extension parameters keep the default fixture branch unchanged.

    The scale version calls this once per day: `initialize` is only on for the first day (inventory opening),
    `finalize` only for the last day (inbound + inventory snapshot). Running state across days
    (opening, balance, emitted listings) is kept on `e` itself.
    """
    at = lambda days, hours=0: _stamp(start + timedelta(days=days, hours=hours))  # noqa: E731
    _emit_openings(e, at, catalog or ("sku-A", "sku-B"), opening_quantity, initialize)
    for order in e.world["orders"]:
        for index, line in enumerate(order["lines"]):
            _emit_line(e, at, start, config, order, index, line, delivery_parts)
    if listing_corrections:
        _emit_listing_corrections(e, at)
    if finalize:
        _emit_inventory_close(e, at, inventory_day)


def _emit_openings(e, at, catalog, opening_quantity, initialize):
    """Inventory opening for each SKU (only with `initialize`), then initialize balance/listing on `e`."""
    openings = getattr(e, "openings", {})
    for sku in catalog if initialize else ():
        p = {
            "seller_id": "seller-1",
            "pool_id": "seller-shared-1",
            "location_id": "warehouse-1",
            "internal_sku_id": sku,
            "binding_version": "pool-binding-1",
            "quantity": opening_quantity,
            "operation_id": f"opening-{sku}",
            "operation_revision": 1,
            "operation_type": "opening",
            "depends_on_operation_ids": [],
            "delta_on_hand": opening_quantity,
            "delta_reserved": 0,
            "delta_non_sellable": 0,
        }
        openings[sku] = e.add("seller_wms.stock_operations", p["operation_id"], p, at(0))
    balances = getattr(e, "balances", None) or {
        sku: {"on_hand": opening_quantity, "reserved": 0, "non_sellable": 0} for sku in openings
    }
    e.openings, e.balances = openings, balances
    e.listings = getattr(e, "listings", set())


def _emit_line(e, at, start, config, original_order, index, line, delivery_parts):
    """Lifecycle of an Order Line: commitment → reservation → (partial cancel) → delivery → after-sales."""
    scope = {
        "seller_id": "seller-1",
        "channel": original_order["channel"],
        "account_id": original_order["account_id"],
        "native_order_id": original_order["native_order_id"],
    }
    order = {**original_order, **line.get("schedule", {})}
    key = stable_id(
        "line",
        e.dataset,
        "seller-1",
        order["channel"],
        order["account_id"],
        order["native_order_id"],
        {"native_line_id": line["native_line_id"]},
    )
    base = {**scope, "native_line_id": line["native_line_id"], "line_id": key}
    listing = _emit_listing_once(e, at, scope, order["channel"], line["sku"])
    binding = stable_id("binding", key, listing, "mapping-1")
    alloc = stable_id("allocation", line["physical_line_id"])
    reservation = stable_id("reservation", line["physical_line_id"])
    canceled = line["canceled"]
    cancel_op = stable_id("cancel", key)
    commitment = {
        **base,
        "ordered_original_qty": 3,
        "ordered_basis_qty": 3,
        "canceled_unshipped_qty": 0,
        "fulfillment_terminal": False,
        "currency": "USD",
        "minor_unit_scale": 2,
        "ordered_gross_minor": 33000,
        "ordered_discount_minor": 3000,
        "ordered_product_net_minor": 30000,
        "canceled_product_net_minor": 0,
        "cancellation_operation_ids": [],
        "price_basis_revision": "price-1",
        "listing_binding_id": binding,
        "internal_sku_id": line["sku"],
        "sale_mapping_version": listing + "-1",
        "allocation_rule_version": "largest-remainder-line-id/1",
    }
    initial = e.add(
        "seller_oms.line_commitments",
        key,
        commitment,
        order["created_at"],
        operation="state_revision",
    )
    stock = _StockChain(
        e,
        key,
        {
            "seller_id": "seller-1",
            "pool_id": "seller-shared-1",
            "location_id": "warehouse-1",
            "internal_sku_id": line["sku"],
            "binding_version": "pool-binding-1",
            "reservation_id": reservation,
            "allocation_id": alloc,
            "line_id": key,
            "operation_revision": 1,
        },
        e.openings[line["sku"]],
        e.balances[line["sku"]],
    )
    stock.add("reserve", 3, 0, 3, 0, order["created_at"])
    units = line["delivery_plan"][0]["physical_unit_ids"]
    allocation = {
        **base,
        "allocation_id": alloc,
        "physical_unit_ids": units,
        "quantity": len(units),
        "shipment_id": stable_id("shipment", alloc),
        "bound_pool_id": "seller-shared-1",
        "bound_location_id": "warehouse-1",
        "binding_version": "pool-binding-1",
        "internal_sku_id": line["sku"],
        "allocation_state": "planned",
    }
    planned = e.add(
        "seller_oms.fulfillment_allocations",
        alloc,
        allocation,
        order["created_at"],
        operation="state_revision",
        depends=[initial["source_event_id"]],
    )
    if canceled:
        stock.add(
            "release",
            1,
            0,
            -1,
            0,
            order["ship_at"],
            {"cancellation_operation_id": cancel_op, "affected_unit_ids": line["units"][-1:]},
        )
        cancel = {
            **commitment,
            "canceled_unshipped_qty": 1,
            "canceled_product_net_minor": 10000,
            "cancellation_operation_ids": [cancel_op],
            "canceled_unit_ids": line["units"][-1:],
            "cancellation_effective_at": order["ship_at"],
            "cancellation_amount_basis": "price-1",
        }
        e.add(
            "seller_oms.line_commitments",
            key,
            cancel,
            order["ship_at"],
            revision=2,
            operation="state_revision",
            depends=[initial["source_event_id"]],
        )
    else:
        cancel = commitment
    if line.get("open", False):
        return
    delivery_payload, delivery_events = _emit_first_shipment(
        e, stock, order, base, scope, alloc, allocation, planned, units, delivery_parts
    )
    terminal_at = order["delivered_at"]
    if len(line["delivery_plan"]) > 1:
        delivery_events.append(
            _emit_second_shipment(e, stock, order, line, allocation, initial, delivery_payload)
        )
        terminal_at = line["delivery_plan"][1]["delivered_at"]
    e.add(
        "seller_oms.line_commitments",
        key,
        {**cancel, "fulfillment_terminal": True},
        terminal_at,
        revision=3 if canceled else 2,
        operation="state_revision",
        depends=[r["source_event_id"] for r in delivery_events],
    )
    if line.get("postsales", index == 0):
        _emit_postsales(
            e,
            at,
            start,
            config,
            stock,
            order,
            line,
            base,
            scope,
            key,
            alloc,
            units,
            delivery_events[0],
        )


def _emit_listing_once(e, at, scope, channel, sku):
    """Opening Listing of (Channel, SKU), emitted exactly once before the first sale."""
    listing = stable_id("listing", channel, sku)
    if listing not in e.listings:
        e.listings.add(listing)
        e.add(
            "seller_catalog.listing_versions",
            listing,
            {
                **scope,
                "listing_id": listing,
                "native_listing_id": listing,
                "internal_sku_id": sku,
                "listing_state": "active",
                "valid_from": at(0),
                "valid_to": None,
                "mapping_version": listing + "-1",
                "change_kind": "business_change",
                "category_id": "category-1",
                "attribute_version": "attributes-1",
                "eligible_pool_ids": ["seller-shared-1"],
                "eligibility_version": "eligibility-1",
            },
            at(0),
            operation="state_revision",
        )
    return listing


def _emit_first_shipment(
    e, stock, order, base, scope, alloc, allocation, planned, units, delivery_parts
):
    """Issue stock, deliver (split into `delivery_parts` events) and move the allocation planned → shipped → delivered."""
    stock.add(
        "ship",
        len(units),
        -len(units),
        -len(units),
        0,
        order["ship_at"],
        {"physical_unit_ids": units},
    )
    shipped = e.add(
        "seller_oms.fulfillment_allocations",
        alloc,
        {**allocation, "allocation_state": "shipped"},
        order["ship_at"],
        revision=2,
        operation="state_revision",
        depends=[planned["source_event_id"]],
    )
    delivery_payload = {
        **base,
        "delivery_event_id": stable_id("delivery", alloc),
        "allocation_id": alloc,
        "delivered_unit_ids": units,
        "delivered_qty": len(units),
        "actual_delivered_at": order["delivered_at"],
        "event_type": "delivered",
        "authority_scope": {"pool_ids": ["seller-shared-1"], **scope},
    }
    delivery_events = []
    for part in range(min(delivery_parts, len(units))):
        part_units = units[part::delivery_parts]
        part_id = stable_id("delivery-part", alloc, part)
        delivery_events.append(
            e.add(
                "carrier.delivery_events",
                part_id,
                {
                    **delivery_payload,
                    "delivery_event_id": part_id,
                    "delivered_unit_ids": part_units,
                    "delivered_qty": len(part_units),
                },
                order["delivered_at"],
                _shift(order["delivered_at"], 1),
                depends=[shipped["source_event_id"]],
            )
        )
    e.add(
        "seller_oms.fulfillment_allocations",
        alloc,
        {**allocation, "allocation_state": "delivered"},
        order["delivered_at"],
        revision=3,
        operation="state_revision",
        depends=[r["source_event_id"] for r in delivery_events],
    )
    return delivery_payload, delivery_events


def _emit_second_shipment(e, stock, order, line, allocation, initial, delivery_payload):
    """The second delivery batch (one unit) has its own allocation; return that batch's delivery event."""
    second_plan = line["delivery_plan"][1]
    second_alloc = stable_id("allocation", line["physical_line_id"], "second-shipment")
    second_payload = {
        **allocation,
        "allocation_id": second_alloc,
        "quantity": 1,
        "physical_unit_ids": second_plan["physical_unit_ids"],
        "shipment_id": stable_id("shipment", second_alloc),
    }
    planned_second = e.add(
        "seller_oms.fulfillment_allocations",
        second_alloc,
        second_payload,
        order["created_at"],
        operation="state_revision",
        depends=[initial["source_event_id"]],
    )
    stock.add(
        "ship-part2",
        1,
        -1,
        -1,
        0,
        second_plan["ship_at"],
        {
            "operation_type": "ship",
            "allocation_id": second_alloc,
            "physical_unit_ids": second_plan["physical_unit_ids"],
        },
    )
    shipped_second = e.add(
        "seller_oms.fulfillment_allocations",
        second_alloc,
        {**second_payload, "allocation_state": "shipped"},
        second_plan["ship_at"],
        revision=2,
        operation="state_revision",
        depends=[planned_second["source_event_id"]],
    )
    did = stable_id("delivery", second_alloc)
    delivered_second = e.add(
        "carrier.delivery_events",
        did,
        {
            **delivery_payload,
            "delivery_event_id": did,
            "allocation_id": second_alloc,
            "delivered_qty": 1,
            "delivered_unit_ids": second_plan["physical_unit_ids"],
            "actual_delivered_at": second_plan["delivered_at"],
        },
        second_plan["delivered_at"],
        _shift(second_plan["delivered_at"], 1),
        depends=[shipped_second["source_event_id"]],
    )
    e.add(
        "seller_oms.fulfillment_allocations",
        second_alloc,
        {**second_payload, "allocation_state": "delivered"},
        second_plan["delivered_at"],
        revision=3,
        operation="state_revision",
        depends=[delivered_second["source_event_id"]],
    )
    return delivered_second


def _emit_postsales(
    e, at, start, config, stock, order, line, base, scope, key, alloc, units, delivery
):
    """After-sales of a line: Return (requested → approved → received) and/or Refund.

    Return and Refund are independent (ADR-0010): `line["return"]` / `line["refund"]` toggle each side separately.
    """
    retid = stable_id("return", key)
    rp = {
        **base,
        "return_id": retid,
        "return_item_id": retid + "-item",
        "allocation_id": alloc,
        "affected_unit_ids": units[:1],
        "quantity": 1,
        "policy_regime_id": "approval-normal-v1",
    }
    requested_at = _shift(order["delivered_at"], 27)
    if line.get("return", True):
        requested = e.add(
            "seller_postsales.return_events",
            retid,
            {**rp, "event_type": "requested"},
            requested_at,
            depends=[delivery["source_event_id"]],
        )
        approved_observed = at(
            (datetime.fromisoformat(order["approved_at"].replace("Z", "+00:00")) - start).days + 3,
            10,
        )
        approved = e.add(
            "seller_postsales.return_events",
            retid,
            {**rp, "event_type": "approved"},
            order["approved_at"],
            approved_observed,
            revision=2,
            depends=[requested["source_event_id"]],
        )
    else:
        requested = approved = delivery
    if line.get("refund", True):
        _emit_refund(
            e, at, config, order, line, base, scope, key, alloc, requested, approved, requested_at
        )
    if line.get("return", True):
        receipt = stock.add(
            "return_receive",
            1,
            1,
            0,
            1,
            line.get("receive_at", at(40)),
            {"receipt_id": retid + "-receipt", "physical_unit_ids": units[:1]},
        )
        stock.add(
            "disposition",
            1,
            0,
            0,
            -1,
            line.get("disposition_at", at(41)),
            {"receipt_id": retid + "-receipt", "disposition": "restockable"},
        )
        e.add(
            "seller_postsales.return_events",
            retid,
            {**rp, "event_type": "received", "receipt_id": retid + "-receipt"},
            line.get("receive_at", at(40)),
            revision=3,
            depends=[approved["source_event_id"], receipt["source_event_id"]],
        )


def _emit_refund(
    e, at, config, order, line, base, scope, key, alloc, requested, approved, requested_at
):
    """Product Refund of a line, in one of two cases.

    * `refund_correction=False`: the refund attaches directly to the line (may fail once, then retry).
    * `refund_correction=True` (default): the first refund does not know its line (uncertainty), then
      a correction reattaches it to the right line; plus an unallocated shipping-fee refund.
    """
    refundid = stable_id("refund", key)
    refund = {
        **base,
        "refund_transaction_id": refundid,
        "transaction_revision": 1,
        "status": "success",
        "component": "product",
        "amount_minor": 10000,
        "currency": "USD",
        "minor_unit_scale": 2,
        "transaction_total_minor": 11000 if line.get("refund_correction", True) else 10000,
        "unmatched_amount_minor": 0,
        "allocation_money_rows": [
            {
                "allocation_id": alloc,
                "amount_minor": 10000,
                "evidence_ref": approved["source_event_id"],
            }
        ],
    }
    if not line.get("refund_correction", True):
        failed = None
        if line.get("refund_retry", False):
            failed = e.add(
                "seller_postsales.refund_events",
                refundid + "-product",
                {**refund, "status": "failed"},
                requested_at,
                operation="state_revision",
                depends=[requested["source_event_id"]],
            )
        e.add(
            "seller_postsales.refund_events",
            refundid + "-product",
            {**refund, "transaction_revision": 2 if failed else 1},
            order["approved_at"],
            revision=2 if failed else 1,
            operation="state_revision",
            depends=[approved["source_event_id"]] + ([failed["source_event_id"]] if failed else []),
        )
        return
    uncertainty_id = stable_id("uncertainty", key)
    ambiguous = e.add(
        "seller_postsales.refund_events",
        refundid + "-product",
        {
            **refund,
            "native_line_id": None,
            "line_id": None,
            "allocation_money_rows": [],
            "unmatched_amount_minor": 10000,
            "unresolved_scope_ref": uncertainty_id,
        },
        order["approved_at"],
        depends=[approved["source_event_id"]],
    )
    correction_time = line.get("correction_at", at(config.creation_days + 40))
    correction = e.add(
        "seller_postsales.refund_events",
        refundid + "-product",
        {**refund, "transaction_revision": 2, "correction_reason": "allocation_evidence_received"},
        order["approved_at"],
        correction_time,
        revision=2,
        operation="correction",
        corrects=ambiguous["source_event_id"],
        depends=[ambiguous["source_event_id"], approved["source_event_id"]],
    )
    uncertainty = {
        "uncertainty_id": uncertainty_id,
        "source_event_ref": ambiguous["source_event_id"],
        "dataset_version": e.dataset,
        "producer": "sim.seller.postsales",
        "clock_domain": CLOCK,
        "reason": "missing_allocation",
        "affected_scope": {**scope, "candidate_native_line_ids": ["1", "2"]},
        "possible_effective_interval": [order["approved_at"], _shift(order["approved_at"], 1)],
        "unallocated_product_amount_minor": 10000,
        "currency": "USD",
        "resolution_state": "unresolved",
        "source_observed_at": order["approved_at"],
        "revision": 1,
    }
    e.uncertainties.extend(
        [
            uncertainty,
            {
                **uncertainty,
                "resolution_state": "resolved",
                "revision": 2,
                "resolution_event_ref": correction["source_event_id"],
                "source_observed_at": correction_time,
            },
        ]
    )
    e.add(
        "seller_postsales.refund_events",
        refundid + "-shipping",
        {**refund, "component": "shipping", "amount_minor": 1000, "allocation_money_rows": []},
        order["approved_at"],
        depends=[approved["source_event_id"]],
    )


def _emit_listing_corrections(e, at):
    """Amazon Listing sku-A changes pool (revision 2), then is corrected for a wrong SKU mapping (correction)."""
    listing = stable_id("listing", "amazon", "sku-A")
    opening_listing = next(
        r for r in e.rows["seller_catalog.listing_versions"] if r["entity_id"] == listing
    )
    changed = e.add(
        "seller_catalog.listing_versions",
        listing,
        {
            **opening_listing["payload"],
            "valid_from": at(1, 12),
            "mapping_version": listing + "-2",
            "eligible_pool_ids": ["seller-secondary-1"],
            "eligibility_version": "eligibility-2",
        },
        at(1, 12),
        at(4),
        revision=2,
        operation="state_revision",
        depends=[opening_listing["source_event_id"]],
    )
    e.add(
        "seller_catalog.listing_versions",
        listing,
        {
            **changed["payload"],
            "internal_sku_id": "sku-B",
            "mapping_version": listing + "-3",
            "change_kind": "source_correction",
            "correction_reason": "mapping_in_change_was_incorrect",
        },
        at(1, 12),
        at(6),
        revision=3,
        operation="correction",
        corrects=changed["source_event_id"],
        depends=[changed["source_event_id"]],
    )


def _emit_inventory_close(e, at, inventory_day):
    """End of period: each SKU receives an inbound batch (5 promised, 2 received, closed 3 short), then inventory is snapshotted."""
    for sku, balance in e.balances.items():
        inbound = "inbound-" + sku
        promise = e.add(
            "seller_wms.inbound_events",
            inbound,
            {
                "seller_id": "seller-1",
                "inbound_line_id": inbound,
                "pool_id": "seller-shared-1",
                "location_id": "warehouse-1",
                "internal_sku_id": sku,
                "event_type": "promised",
                "quantity": 5,
                "eta": at(inventory_day),
            },
            at(0),
        )
        received = e.add(
            "seller_wms.inbound_events",
            inbound,
            {
                **promise["payload"],
                "event_type": "received",
                "quantity": 2,
                "receipt_id": inbound + "-receipt",
            },
            at(inventory_day),
            revision=2,
            depends=[promise["source_event_id"]],
        )
        opid = inbound + "-receipt-op"
        e.add(
            "seller_wms.stock_operations",
            opid,
            {
                "seller_id": "seller-1",
                "operation_id": opid,
                "operation_revision": 1,
                "operation_type": "inbound_receipt",
                "pool_id": "seller-shared-1",
                "location_id": "warehouse-1",
                "internal_sku_id": sku,
                "binding_version": "pool-binding-1",
                "inbound_line_id": inbound,
                "receipt_id": inbound + "-receipt",
                "quantity": 2,
                "delta_on_hand": 2,
                "delta_reserved": 0,
                "delta_non_sellable": 0,
                "depends_on_operation_ids": [e.openings[sku]["payload"]["operation_id"]],
            },
            at(inventory_day),
            depends=[received["source_event_id"], e.openings[sku]["source_event_id"]],
        )
        e.add(
            "seller_wms.inbound_events",
            inbound,
            {**received["payload"], "event_type": "closed", "unreceived_qty": 3},
            at(inventory_day + 1),
            revision=3,
            depends=[received["source_event_id"]],
        )
        e.add(
            "seller_wms.stock_snapshots",
            "snapshot-" + sku,
            {
                "seller_id": "seller-1",
                "pool_id": "seller-shared-1",
                "location_id": "warehouse-1",
                "internal_sku_id": sku,
                **balance,
                "on_hand": balance["on_hand"] + 2,
            },
            at(inventory_day + 2),
            operation="state_revision",
        )


def _fact_digest(row):
    """Digest of a row without execution metadata, to detect true duplicates."""
    # recorded_at is the machine run time, not business truth or a source observation.
    return digest({k: v for k, v in row.items() if k != "recorded_at"})


def inspect_records(records):
    """Check source identities, revisions and causal references, without applying truth.

    Input order is transport order. Missing references/revisions are left pending; this function
    does not compute inventory, reconcile quantities, or build labels.
    """
    unique = {}
    duplicates = 0
    conflicts = set()
    entities = defaultdict(dict)
    for row in records:
        key = (row["dataset_version"], row["producer"], row["source_epoch"], row["source_event_id"])
        if key in unique:
            if _fact_digest(unique[key]) == _fact_digest(row):
                duplicates += 1
            else:
                conflicts.add(row["source_event_id"])
            continue
        unique[key] = row
        entity = (
            row["dataset_version"],
            row["producer"],
            row["source_epoch"],
            row["feed"],
            row["entity_id"],
        )
        rev = row["entity_revision"]
        if rev in entities[entity]:
            conflicts.add(row["source_event_id"])
        entities[entity][rev] = row
    revision_gaps = []
    for revisions in entities.values():
        if sorted(revisions) != list(range(1, max(revisions) + 1)):
            revision_gaps.extend(r["source_event_id"] for r in revisions.values())
    ids = {r["source_event_id"]: r for r in unique.values()}
    pending = set()
    for row in unique.values():
        references = list(row["depends_on_event_ids"])
        if row["operation"] in ("correction", "invalidation"):
            target = row.get("corrects_source_event_id")
            if (
                not target
                or not row["payload"].get("correction_reason")
                or target in ids
                and ids[target]["entity_revision"] >= row["entity_revision"]
            ):
                conflicts.add(row["source_event_id"])
            if target:
                references.append(target)
        if any(ref not in ids for ref in references):
            pending.add(row["source_event_id"])
    changed = True
    while changed:
        before = len(pending)
        for row in unique.values():
            if set(row["depends_on_event_ids"]) & (pending | conflicts | set(revision_gaps)):
                pending.add(row["source_event_id"])
        changed = len(pending) != before
    status = (
        "conflict"
        if conflicts
        else "pending_revision"
        if revision_gaps
        else "pending_reference"
        if pending
        else "ready"
    )
    return {
        "status": status,
        "duplicates": duplicates,
        "unique_records": len(unique),
        "conflict_event_ids": sorted(conflicts),
        "pending_event_ids": sorted(pending),
        "revision_gap_event_ids": sorted(revision_gaps),
    }


def _identity_probes(e):
    """Build diagnostic cases (quantity change, absence, correction) to exercise identity handling."""
    operations = e.rows["seller_wms.stock_operations"]
    reserve = next(r for r in operations if r["payload"]["operation_type"] == "reserve")
    opening = next(
        r for r in operations if r["source_event_id"] == reserve["depends_on_event_ids"][0]
    )
    release = next(r for r in operations if r["payload"]["operation_type"] == "release")
    altered = {**reserve, "payload": {**reserve["payload"], "quantity": 999}}
    second = next(
        r for r in e.rows["seller_oms.fulfillment_allocations"] if r["entity_revision"] == 2
    )
    order = next(o for o in e.world["orders"] if o["channel"] == "shopify")
    native = _native(order)
    native["order"]["line_items"][0]["current_quantity"] = 2
    absent = {"order": {**native["order"], "line_items": []}}
    line = next(
        r
        for r in e.rows["seller_oms.line_commitments"]
        if r["payload"]["native_order_id"] == order["native_order_id"]
        and r["payload"]["native_line_id"] == "1"
    )
    correction = {
        **opening,
        "source_event_id": stable_id("event", e.dataset, "opening-correction-probe"),
        "entity_revision": 2,
        "operation": "correction",
        "corrects_source_event_id": opening["source_event_id"],
        "depends_on_event_ids": [opening["source_event_id"]],
        "source_sequence": e.sequences["sim.seller.wms"] + 1,
        "source_observed_at": line["source_observed_at"],
        "payload": {
            **opening["payload"],
            "operation_revision": 2,
            "quantity": 199,
            "delta_on_hand": 199,
            "correction_reason": "opening_count_was_incorrect",
        },
    }
    return [
        {
            "case": "native_quantity_then_absence",
            "records": [native, absent],
            "line_ref": line["payload"]["line_id"],
            "snapshot_manifest": {
                "snapshot_mode": "full_entity",
                "snapshot_scope": {"order_ids": [order["native_order_id"]]},
                "snapshot_as_of": line["source_observed_at"],
                "source_observed_at": line["source_observed_at"],
                "schema_id": "sim-native-v2/shopify/1",
                "page_set_complete": True,
                "file_set_digest": digest([native, absent]),
            },
            "expected_behavior": "preserve ordered basis3 and all shipment/stock facts; no inferred cancellation/delete",
        },
        {
            "case": "stock_correction_reordered",
            "records": [correction, opening],
            "expected_behavior": "latest revision replaces opening delta; projection must not add both revisions",
        },
        {"case": "retry", "records": [opening, reserve, reserve]},
        {"case": "hash_conflict", "records": [opening, reserve, altered]},
        {"case": "release_before_reserve", "records": [release, reserve, opening]},
        {"case": "missing_dependency", "records": [release]},
        {"case": "revision_gap", "records": [second]},
    ]


def coverage_receipt(rows, envelope):
    """Check an emitted, frozen set; complete data does not remove link ambiguity.

    Expected sequence ranges and partitions come from the producer envelope, not from received rows.
    This is a source receipt, not a Data Platform validator.
    """
    if envelope.get("capability_status") == "unavailable":
        return {"status": "unavailable", "reasons": ["missing_capability"]}
    reasons = set()
    expected = {
        (span["partition"], n)
        for span in envelope["source_sequence_ranges"]
        for n in range(span["first"], span["last"] + 1)
    }
    actual, identities = set(), set()
    for row in rows:
        try:
            actual.add((row["source_partition"], row["source_sequence"]))
            identity = (
                row["dataset_version"],
                row["producer"],
                row["source_epoch"],
                row["source_event_id"],
            )
            if identity in identities:
                reasons.add("duplicate_source_identity")
            identities.add(identity)
            if any(
                row[k] != envelope[k]
                for k in ("dataset_version", "producer", "source_epoch", "feed")
            ):
                reasons.add("scope_identity_mismatch")
            if row["source_observed_at"] >= envelope["source_observed_at"]:
                reasons.add("not_yet_observed")
            interval = envelope["event_interval"]
            if not interval["start_inclusive"] <= row["effective_at"] < interval["end_exclusive"]:
                reasons.add("outside_event_interval")
        except (KeyError, TypeError):
            reasons.add("unparseable_record")
    if expected - actual:
        reasons.add("missing_sequence")
    if actual - expected:
        reasons.add("unexpected_sequence")
    if set(envelope["expected_partitions"]) != set(envelope["completed_partitions"]):
        reasons.add("missing_partition")
    if not {p for p, _ in expected} <= set(envelope["completed_partitions"]):
        reasons.add("missing_partition")
    if envelope["source_record_count"] != len(rows):
        reasons.add("record_count_mismatch")
    if any(not isinstance(r, dict) or "source_event_id" not in r for r in rows):
        reasons.add("unparseable_record")
    elif digest(sorted(rows, key=lambda r: r["source_event_id"])) != envelope["record_set_sha256"]:
        reasons.add("record_set_digest_mismatch")
    if envelope["status"] != "complete":
        reasons.add("producer_interval_not_closed")
    return {"status": "partial" if reasons else "complete", "reasons": sorted(reasons)}


def _check_fixture_args(delivery_parts, native_schema, native_schemas):
    """Reject renderer combinations the fixture does not support."""
    if delivery_parts not in (1, 2) or native_schema not in (1, 2):
        raise ValueError("fixture renderer supports delivery_parts/native_schema 1 or 2")
    if any(
        c not in CHANNELS
        or v not in (1, 2)
        or (v == 2 and c not in ("amazon", "shopee", "tiktok_shop"))
        for c, v in native_schemas.items()
    ):
        raise ValueError("unsupported native schema override")


def _fixture_identity(config, world, delivery_parts, native_schema, native_schemas):
    """Everything that determines `dataset_version`; the renderer hash is pinned for the default branch."""
    identity = {
        "configuration": asdict(config),
        "world_sha256": digest(world),
        # The source contract hash is pinned like generator_sha256: editing links/names in the doc must not change dataset_version.
        "contract_sha256": "84fa3bb73775e418e8e7ea5fc69be20f4f9324e99abc7f43a9aee0af7faacabf",
        "source_profiles": PROFILES,
        "fact_hash_policy": "canonical envelope excluding execution-only recorded_at; coverage hashes full emitted rows",
        # The original renderer hash is pinned to keep the default dataset identity; overrides use the real code hash.
        "generator_sha256": (
            hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
            if native_schemas
            else "655021eedf0f66fe3d765a4eb849049d1661ba2845ff2b4e76b4e54ebac040b2"
        ),
        "runtime": {"python": platform.python_version()},
        "schema_version": "seller-source-v2/1",
        "module_versions": {
            "world": "physical-world-v2/1",
            "renderer": f"fixture-native/{native_schema}",
        },
        "renderer_configuration": {
            "delivery_parts": delivery_parts,
            "native_schema": native_schema,
        },
    }
    if native_schemas:
        identity["renderer_configuration"]["native_schemas"] = dict(sorted(native_schemas.items()))
    return identity


def _propagate_known_time(e):
    """A downstream producer cannot know an observation earlier than the event it references.

    Iterate to a fixed point: each row's `source_observed_at` = max of itself and every row it depends on.
    Uncertainty is also pushed along with the event it points to. Return every source row.
    """
    all_rows = [r for rows in e.rows.values() for r in rows]
    by_id = {r["source_event_id"]: r for r in all_rows}
    for _ in range(len(all_rows)):
        changed = False
        for row in all_rows:
            known = max(
                [row["source_observed_at"]]
                + [by_id[ref]["source_observed_at"] for ref in row["depends_on_event_ids"]]
            )
            if known != row["source_observed_at"]:
                row["source_observed_at"] = known
                changed = True
        if not changed:
            break
    for u in e.uncertainties:
        ref = u.get("resolution_event_ref", u["source_event_ref"])
        u["source_observed_at"] = max(u["source_observed_at"], by_id[ref]["source_observed_at"])
    return all_rows


def _resequence_by_observation(e):
    """Producer sequence by observation clock, not by render order."""
    by_producer = defaultdict(list)
    for rows in e.rows.values():
        for row in rows:
            by_producer[row["producer"]].append(row)
    for rows in by_producer.values():
        for sequence, row in enumerate(
            sorted(rows, key=lambda r: (r["source_observed_at"], r["source_sequence"])), 1
        ):
            row["source_sequence"] = sequence
    for rows in e.rows.values():
        rows.sort(key=lambda r: r["source_sequence"])


def _delay_seconds(row):
    """Observation delay of a row: `source_observed_at − effective_at`, in seconds."""
    return (
        datetime.fromisoformat(row["source_observed_at"].replace("Z", "+00:00"))
        - datetime.fromisoformat(row["effective_at"].replace("Z", "+00:00"))
    ).total_seconds()


class _PartitionWriter:
    """Write each JSONL partition under `root` and keep file metadata for the manifest."""

    def __init__(self, root, dataset):
        """Nothing written yet; `files` grows with each `write`."""
        self.root, self.dataset, self.files = root, dataset, []

    def write(self, feed, producer, rows, export_date):
        """Write one JSONL partition and record its path, size and sha256 in the manifest."""
        rel = f"dataset_version={self.dataset}/producer={producer}/feed={feed}/export_date={export_date}/part-00000.jsonl"
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        data = "".join(canonical(row) + "\n" for row in rows).encode()
        path.write_bytes(data)
        self.files.append(
            {
                "path": rel,
                "role": "diagnostic_fixture" if feed == "fixture.probes" else "source",
                "feed": feed,
                "producer": producer,
                "rows": len(rows),
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
        return self.files[-1]


def _write_native_snapshots(writer, world, native_schema, native_schemas, horizons):
    """Write native orders per Channel (one file per Channel) and return the snapshot description for the manifest.

    Shopee schema 2 renames keys (breaking); TikTok Shop schema 2 adds optional fields (compatible).
    """
    native_sources = {}
    for channel in CHANNELS:
        orders = [o for o in world["orders"] if o["channel"] == channel]
        feed = channel + ".orders"
        version = native_schemas.get(channel, native_schema if channel == "amazon" else 1)
        rendered = []
        for index, order in enumerate(orders):
            row = _native(order, version if channel == "amazon" else 1)
            if channel == "shopee" and version == 2:
                body = row["response"]["order_list"][0]
                body["order_number"] = body.pop("order_sn")
                for item in body["item_list"]:
                    item["sku"] = item.pop("model_sku")
            if channel == "tiktok_shop" and version == 2 and index % 2 == 0:
                row["data"]["orders"][0]["buyer_message"] = "Please pack carefully"
            rendered.append(row)
        file = writer.write(
            feed, "sim.native." + channel, rendered, horizons["order_creation_end"][:10]
        )
        native_sources[channel] = {
            "transport": "postgres_bootstrap_only" if channel == "walmart" else "object_store_file",
            "ingestion_boundary": "jdbc" if channel == "walmart" else "file",
            "snapshot_mode": "full_entity",
            "snapshot_scope": {"order_ids": [o["native_order_id"] for o in orders]},
            "snapshot_as_of": horizons["order_creation_end"],
            "page_set_complete": True,
            "schema_id": f"sim-native-v2/{channel}/{version}",
            "file_set_digest": file["sha256"],
            "source_observed_at": horizons["order_creation_end"],
            "account_id": f"account-{channel}-1",
        }
    return native_sources


def _fixture_coverage(e, dataset, start, config, horizons):
    """Fixture authority and coverage: one full-period coverage per feed, returning (authority, coverage).

    Refund also has revision 2 (after the correction resolves the uncertainty). Feeds of a Channel-managed
    pool also get an `unavailable` coverage.
    """
    authority = []
    coverage = []
    for feed, (producer_short, capability, profile) in FEEDS.items():
        producer = "sim.seller." + producer_short
        authority.append(
            {
                "authority_id": stable_id("authority", dataset, feed),
                "producer": producer,
                "feed": feed,
                "capability": capability,
                "profile_id": profile,
                "status": "available",
                "scope": {
                    "seller_id": "seller-1",
                    "account_ids": [f"account-{c}-1" for c in CHANNELS],
                    "channels": list(CHANNELS),
                    "pool_ids": ["seller-shared-1"],
                },
                "valid_from": horizons["order_creation_start"],
                "valid_to": horizons["source_observation_end"],
            }
        )
        observed = _stamp(
            start
            + timedelta(
                days=config.creation_days + 38
                if feed == "seller_postsales.refund_events"
                else config.followup_days + 1
            )
        )
        rows = [r for r in e.rows[feed] if r["source_observed_at"] < observed]
        coverage.append(
            {
                "coverage_id": stable_id("coverage", dataset, feed),
                "producer": producer,
                "feed": feed,
                "dataset_version": dataset,
                "source_epoch": producer + ":" + dataset,
                "profile_id": profile,
                "scope": authority[-1]["scope"],
                "capability": capability,
                "event_interval": {
                    "start_inclusive": horizons["order_creation_start"],
                    "end_exclusive": _stamp(start + timedelta(days=config.creation_days + 35))
                    if feed == "seller_postsales.refund_events"
                    else horizons["lifecycle_followup_end"],
                },
                "status": "complete",
                "expected_partitions": ["partition-0"],
                "completed_partitions": ["partition-0"],
                "source_sequence_ranges": [
                    {
                        "partition": "partition-0",
                        "first": r["source_sequence"],
                        "last": r["source_sequence"],
                    }
                    for r in rows
                ],
                "source_record_count": len(rows),
                "record_set_sha256": digest(sorted(rows, key=lambda r: r["source_event_id"])),
                "unresolved_scope_refs": [
                    u["uncertainty_id"] for u in e.uncertainties if u["revision"] == 1
                ]
                if feed == "seller_postsales.refund_events"
                else [],
                "source_observed_at": observed,
                "clock_domain": CLOCK,
                "revision": 1,
            }
        )
    refund_coverage = next(c for c in coverage if c["feed"] == "seller_postsales.refund_events")
    refund_rows = e.rows["seller_postsales.refund_events"]
    coverage.append(
        {
            **refund_coverage,
            "revision": 2,
            "source_observed_at": _stamp(start + timedelta(days=config.creation_days + 42)),
            "unresolved_scope_refs": [],
            "source_record_count": len(refund_rows),
            "source_sequence_ranges": [
                {
                    "partition": "partition-0",
                    "first": r["source_sequence"],
                    "last": r["source_sequence"],
                }
                for r in refund_rows
            ],
            "record_set_sha256": digest(sorted(refund_rows, key=lambda r: r["source_event_id"])),
        }
    )
    for feed in (
        "carrier.delivery_events",
        "seller_postsales.return_events",
        "seller_postsales.refund_events",
        "seller_wms.stock_operations",
        "seller_wms.inbound_events",
        "seller_wms.stock_snapshots",
    ):
        producer_short, capability, _ = FEEDS[feed]
        scope = {
            "seller_id": "seller-1",
            "account_ids": ["channel-managed-account"],
            "channels": ["amazon"],
            "pool_ids": ["channel-managed-1"],
        }
        authority.append(
            {
                "authority_id": stable_id("authority-unavailable", dataset, feed),
                "producer": "sim.seller." + producer_short,
                "feed": feed,
                "capability": capability,
                "profile_id": "channel-orders-limited/1",
                "status": "unavailable",
                "scope": scope,
                "reason": "channel-managed pool has no simulated operational authority",
                "valid_from": horizons["order_creation_start"],
                "valid_to": horizons["source_observation_end"],
            }
        )
        base_cov = next(c for c in coverage if c["feed"] == feed)
        coverage.append(
            {
                **base_cov,
                "coverage_id": stable_id("coverage-unavailable", dataset, feed),
                "scope": scope,
                "profile_id": "channel-orders-limited/1",
                "status": "unavailable",
                "capability_status": "unavailable",
                "expected_partitions": [],
                "completed_partitions": [],
                "source_sequence_ranges": [],
                "source_record_count": 0,
                "record_set_sha256": digest([]),
                "unresolved_scope_refs": [],
            }
        )
    for cov in coverage:
        if cov["status"] == "complete":
            known = [
                r
                for r in e.rows[cov["feed"]]
                if r["source_observed_at"] < cov["source_observed_at"]
            ]
            receipt = coverage_receipt(known, cov)
            if receipt["status"] != "complete":
                raise ValueError(f"invalid source coverage: {receipt}")
    return authority, coverage


def generate_fixture(
    output_root,
    config=None,
    *,
    delivery_parts=1,
    native_schema=1,
    native_schemas: dict[str, int] | None = None,
):
    """Freeze sources to a new local root; no resume, restart, remote IO or services.

    Steps: emit events → push known-time along dependencies → assign sequence by observation →
    write business feeds, probes, native orders → coverage + uncertainty → manifest.
    """
    began = time.perf_counter()
    native_schemas = native_schemas or {}
    _check_fixture_args(delivery_parts, native_schema, native_schemas)
    config = config or FixtureConfig()
    world = _world(config)
    if "://" in str(output_root):
        raise ValueError("source-v2 fixture requires a local output root")
    root = Path(output_root)
    identity = _fixture_identity(config, world, delivery_parts, native_schema, native_schemas)
    dataset = stable_id("dataset", identity)
    recorded = _stamp(datetime.now(timezone.utc))
    e = _Emitter(world, dataset, recorded)
    start = config.validate()
    _emit_sources(e, start, config, delivery_parts)
    horizons = {
        "clock_domain": CLOCK,
        "order_creation_start": _stamp(start),
        "order_creation_end": _stamp(start + timedelta(days=config.creation_days)),
        "lifecycle_followup_end": _stamp(start + timedelta(days=config.followup_days)),
        "source_observation_end": _stamp(start + timedelta(days=config.observation_days)),
    }
    all_rows = _propagate_known_time(e)
    if any(
        not horizons["order_creation_start"]
        <= r["effective_at"]
        < horizons["lifecycle_followup_end"]
        or not r["effective_at"] <= r["source_observed_at"] < horizons["source_observation_end"]
        for r in all_rows
    ):
        raise ValueError("source event overflow: fixture horizons cannot claim complete")
    _resequence_by_observation(e)
    if any(_delay_seconds(r) > 15 * 86400 for r in all_rows):
        raise ValueError("fixture source delay exceeds declared 15-day cap")
    root.mkdir(parents=True, exist_ok=False)
    pinned = {
        "dataset_version": dataset,
        "world_id": world["world_id"],
        "identity": identity,
        "horizons": horizons,
    }
    (root / "identity.json").write_text(json.dumps(pinned, ensure_ascii=False, indent=2) + "\n")
    writer = _PartitionWriter(root, dataset)
    for feed, rows in list(e.rows.items()):
        groups = defaultdict(list)
        for row in rows:
            groups[row["source_observed_at"][:10]].append(row)
        for day, group in sorted(groups.items()):
            writer.write(feed, rows[0]["producer"], group, day)
    writer.write(
        "fixture.probes",
        "sim.fixture.diagnostics",
        _identity_probes(e),
        horizons["source_observation_end"][:10],
    )
    native_sources = _write_native_snapshots(writer, world, native_schema, native_schemas, horizons)
    authority, coverage = _fixture_coverage(e, dataset, start, config, horizons)
    for feed, producer, rows in (
        ("source.coverage_envelopes", "sim.seller.coverage", coverage),
        ("source.uncertainties", "sim.seller.postsales", e.uncertainties),
    ):
        groups = defaultdict(list)
        for row in rows:
            owner = row.get("producer", producer)
            row.setdefault("schema_id", f"seller-source-v2/{feed}/1")
            row.setdefault("world_id", world["world_id"])
            row.setdefault("recorded_at", recorded)
            groups[(row["source_observed_at"][:10], owner)].append(row)
        for (day, owner), group in sorted(groups.items()):
            writer.write(feed, owner, group, day)
    files = writer.files
    manifest = {
        "contract_id": "seller-source-v2",
        "dataset_version": dataset,
        "world_id": world["world_id"],
        **identity,
        "physical_outcome_sha256": digest(world),
        "horizons": horizons,
        "intent_sha256": digest(
            [
                {"line": line["physical_line_id"], "affected_unit": line["intent_unit"]}
                for order in world["orders"]
                for line in order["lines"]
            ]
        ),
        "authority_capabilities": authority,
        "native_sources": native_sources,
        "files": files,
        "counts": {"orders": len(world["orders"]), "skus": 2, "lines": len(world["orders"]) * 2},
        "recorded_at": recorded,
        "execution_clock_domain": "execution_utc",
        "tail_diagnostics": {
            "right_censored_lines": 0,
            "terminal_lines": len(world["orders"]) * 2,
            "maturity_deadline_passed_lines": len(world["orders"]) * 2,
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
                "max_source_delay_seconds": max(int(_delay_seconds(r)) for r in all_rows),
            },
            "cohorts": [
                {
                    "order_date": _stamp(start + timedelta(days=day))[:10],
                    "orders": 5,
                    "terminal_lines": 10,
                    "right_censored_lines": 0,
                }
                for day in range(config.creation_days)
            ],
            "reason": "bounded deterministic scenarios; maturity counts are not official label eligibility",
        },
        "runtime_seconds": round(time.perf_counter() - began, 6),
        "source_bytes": sum(f["bytes"] for f in files),
        "currency_table": {"version": "fixture-currency/1", "USD": {"minor_unit_scale": 2}},
        "native_snapshot_policy": "frozen creation observations; no native status refresh or actual-delivery claim",
        "scenario_distribution": {
            "type": "deterministic_exhaustive_fixture",
            "random_sampling": False,
            "lines_per_order": 2,
            "line_1": "ordered3/canceled1/delivered2/approved1/refund-product100+shipping10",
            "line_2": "ordered3/two shipments (2+1); candidate uncertainty until correction",
            "intent_identity": "physical population + stable line/unit, independent of approval timing and render count",
        },
        "limits": [
            "local small fixture only",
            "Walmart not loaded: bootstrap is not JDBC proof",
            "source export only; no Bronze/Silver/labels/publication claim",
        ],
    }
    (root / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    return manifest


def require_same_world(left, right):
    """Refuse comparisons based on the seed alone: both the physical identity and the digest must match."""
    fields = ("world_id", "physical_outcome_sha256")
    if any(not left.get(k) or left.get(k) != right.get(k) for k in fields):
        raise ValueError(
            "same-world comparison requires matching physical world identity and outcome digest"
        )


def main():
    """Generate the fixture with per-Channel schema overrides."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--native-schema-for", action="append", default=[], metavar="CHANNEL=N")
    args = parser.parse_args()
    try:
        overrides = {c: int(v) for c, v in (arg.split("=", 1) for arg in args.native_schema_for)}
    except ValueError:
        parser.error("--native-schema-for needs CHANNEL=N")
    manifest = generate_fixture(args.output, native_schemas=overrides)
    print(
        json.dumps(
            {
                "output": args.output,
                "dataset_version": manifest["dataset_version"],
                "counts": manifest["counts"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
