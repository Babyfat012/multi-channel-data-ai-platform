"""Batch renderer for the Shopify REST Admin Order.

One JSONL row simulates the `GET /orders/{id}.json` response, with `order` and the
`line_items` array. The REST Admin API is now legacy; this shape was chosen because `docs/data-platform/reference/channel_sources.md`
already records its schema and both ways of storing Orders use the same core data. No
`delivered_at`: the Order resource only reports fulfillment, not the actual delivery date.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from generator.channels.base import OBJECT_STORE, RenderedRecord, iso_z, money, native_digits
from generator.config import GeneratorConfig
from generator.core.model import (
    CANCELLED,
    COMPLETED,
    DELIVERED,
    PENDING_PAYMENT,
    SHIPPED,
    Order,
    OrderLine,
)
from generator.core.storefronts import CURRENCY_DECIMALS


class ShopifyOrdersRenderer:
    """Render a Shopify Order snapshot with ids stable across every Export Date."""

    channel = "shopify"
    dataset = "orders"
    storage = OBJECT_STORE

    def __init__(self, config: GeneratorConfig) -> None:
        """Store the seed to generate numeric ids like the Shopify API."""
        self.seed = config.seed

    def order_id(self, order: Order) -> int:
        """Generate a fixed numeric Order id for a core Order."""
        return int("5" + native_digits(self.seed, "shopify_order", order.order_id, 12))

    def line_id(self, line: OrderLine) -> int:
        """Generate a fixed numeric Line Item id for a core Order Line."""
        return int("6" + native_digits(self.seed, "shopify_line", line.order_line_id, 12))

    def line_keys(self, order: Order) -> list[dict[str, Any]]:
        """Map core ids to Shopify Order/Line Item ids."""
        order_id = str(self.order_id(order))
        return [
            {
                "order_line_id": line.order_line_id,
                "channel_order_id": order_id,
                "channel_line_id": str(self.line_id(line)),
            }
            for line in order.lines
        ]

    def render(self, order: Order, as_of: datetime, export_day: int) -> RenderedRecord:
        """Render the Order at `as_of`; the Batch layer writes the export date into the path."""
        del export_day
        status_change = order.status_at(as_of)
        status = status_change.status
        decimals = CURRENCY_DECIMALS[order.currency]
        paid = status not in (PENDING_PAYMENT, CANCELLED)
        fulfilled = status in (SHIPPED, DELIVERED, COMPLETED)
        line_items = []
        for line in order.lines:
            line_items.append(
                {
                    "id": self.line_id(line),
                    "sku": line.sku_id,
                    "product_id": int(
                        "7" + native_digits(self.seed, "shopify_product", line.sku_id, 12)
                    ),
                    "variant_id": int(
                        "8" + native_digits(self.seed, "shopify_variant", line.sku_id, 12)
                    ),
                    "title": f"ACME {line.sku_id}",
                    "quantity": line.quantity,
                    "current_quantity": line.quantity,
                    "price": money(line.unit_price, decimals),
                    "total_discount": money(line.unit_discount * line.quantity, decimals),
                    "fulfillment_status": "fulfilled" if fulfilled else None,
                }
            )

        total = sum((line.unit_price - line.unit_discount) * line.quantity for line in order.lines)
        shopify_order: dict[str, Any] = {
            "id": self.order_id(order),
            "name": "#" + native_digits(self.seed, "shopify_name", order.order_id, 8),
            "created_at": iso_z(order.created_at),
            "updated_at": iso_z(status_change.at),
            "closed_at": iso_z(status_change.at) if status == COMPLETED else None,
            "cancelled_at": iso_z(status_change.at) if status == CANCELLED else None,
            "financial_status": "paid" if paid else "voided" if status == CANCELLED else "pending",
            "fulfillment_status": "fulfilled"
            if fulfilled
            else "restocked"
            if status == CANCELLED
            else None,
            "currency": order.currency,
            "total_price": money(total, decimals),
            "customer": (
                None
                if order.buyer.is_guest
                else {
                    "id": int(
                        "9" + native_digits(self.seed, "shopify_customer", order.buyer.buyer_id, 12)
                    ),
                }
            ),
            "shipping_address": {
                "city": order.buyer.city,
                "zip": order.buyer.postal_code,
                "country_code": order.buyer.country,
            },
            "line_items": line_items,
        }
        return RenderedRecord(key=str(shopify_order["id"]), record={"order": shopify_order})
