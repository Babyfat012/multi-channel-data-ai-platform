"""Tests for the Amazon renderer: correct native shape, correct schema per export day."""

from __future__ import annotations

from generator.channels.amazon import AmazonOrdersRenderer
from generator.core.model import FULFILLED_BY_CHANNEL
from generator.core.simulate import Simulator
from generator.delivery.batch_exports import end_of_day


def _orders_of(config, storefront_prefix, days=range(1, 4)):
    """Orders of Storefronts whose id starts with `storefront_prefix`."""
    sim = Simulator(config)
    return [
        o
        for d in days
        for o in sim.simulate_day(d)
        if o.storefront_id.startswith(storefront_prefix)
    ]


def test_amazon_schema_switches_on_configured_day(tiny_config):
    """Prove the exact schema switch day: the same order renders as v0 before it and 2026 from it on, with renames, structure changes and keys present in only one version."""
    renderer = AmazonOrdersRenderer(tiny_config)
    order = _orders_of(tiny_config, "amazon")[0]
    as_of = end_of_day(tiny_config, 8)
    switch = tiny_config.offline_problems.amazon_orders_api_switch_day

    old = renderer.render(order, as_of, export_day=switch - 1).record
    new = renderer.render(order, as_of, export_day=switch).record

    # Rename: same order, differently named key.
    assert old["AmazonOrderId"] == new["orderId"]
    # No new fields: a column present in only one shape is null in the other because the key was renamed
    # (`proceeds` is `OrderTotal` renamed + nested one level, not a new field).
    assert "proceeds" in new and "proceeds" not in old
    assert "IsPrime" in old and "IsPrime" not in new
    # Structure and value spelling change.
    assert new["fulfillment"]["status"] == old["OrderStatus"].upper()


def test_amazon_ids_are_stable_and_well_formed(tiny_config):
    """Prove Amazon order ids are stable across two renderer builds and match the 3-7-7 format."""
    renderer = AmazonOrdersRenderer(tiny_config)
    order = _orders_of(tiny_config, "amazon")[0]
    amazon_id = renderer.amazon_order_id(order)
    assert amazon_id == AmazonOrdersRenderer(tiny_config).amazon_order_id(order)
    assert [len(part) for part in amazon_id.split("-")] == [3, 7, 7]


def test_amazon_never_shows_delivered_and_only_mfn_has_delivery_window(tiny_config):
    """Prove Amazon never shows a 'delivered' status, and only Seller-fulfilled (MFN) orders have an estimated delivery window."""
    renderer = AmazonOrdersRenderer(tiny_config)
    for order in _orders_of(tiny_config, "amazon"):
        record = renderer.render(order, end_of_day(tiny_config, 12), export_day=1).record
        assert record["OrderStatus"] in {"Pending", "Unshipped", "Shipped", "Canceled"}
        has_window = "EarliestDeliveryDate" in record
        assert has_window == (order.fulfillment != FULFILLED_BY_CHANNEL)
