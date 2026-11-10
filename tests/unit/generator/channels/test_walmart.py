"""Walmart native rows and carrier tracking formats."""

import re

import pytest

from generator.channels.walmart import WalmartOrdersRenderer
from generator.delivery.batch_exports import end_of_day
from tests.unit.generator.channels.test_amazon import _orders_of


def test_walmart_row_has_payload_and_fetched_at(tiny_config):
    """Prove a Walmart record has only 3 columns, is keyed by `purchaseOrderId`, and the payload carries the Delivered status with tracking."""
    renderer = WalmartOrdersRenderer(tiny_config)
    delivered = [o for o in _orders_of(tiny_config, "walmart", days=range(1, 6)) if o.delivered_at]
    order = delivered[0]
    as_of = end_of_day(tiny_config, 12)

    rendered = renderer.render(order, as_of, export_day=12)
    row = rendered.record
    assert set(row) == {"purchase_order_id", "raw_payload", "fetched_at"}
    assert rendered.key == row["purchase_order_id"] == row["raw_payload"]["purchaseOrderId"]
    line_status = row["raw_payload"]["orderLines"]["orderLine"][0]["orderLineStatuses"][
        "orderLineStatus"
    ][0]
    assert line_status["status"] == "Delivered"
    assert "trackingNumber" in line_status["trackingInfo"]


@pytest.mark.parametrize(
    "carrier, pattern",
    [
        ("UPS", r"1Z[A-Z0-9]{16}"),
        ("FedEx", r"\d{12}"),
        ("USPS", r"94\d{20}"),
    ],
)
def test_walmart_tracking_number_matches_carrier(tiny_config, carrier, pattern):
    """Prove Walmart tracking numbers match each carrier's format."""
    renderer = WalmartOrdersRenderer(tiny_config)
    assert re.fullmatch(pattern, renderer.tracking_number(carrier, "1445238308495"))
