"""Batch renderer for TikTok Shop Get Order Detail 202309.

Each JSONL row is a `data.orders` response holding exactly one Order. This wrapping
keeps the API shape while still letting the export layer write one record per Order.
The Stream webhook keeps its own renderer in `tiktok.py`; both renderers use the same
Order id, so an Order in Batch and in Stream can be joined.

Order/Line Item fields and statuses follow `docs/data-platform/reference/channel_sources.md` section 5.
Two simplifications in the simulated data: `seller_discount` holds the whole discount
(platform discount is not split out), and an Order has one fulfillment type for all lines.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from generator.channels.base import OBJECT_STORE, RenderedRecord, money, native_digits
from generator.channels.tiktok import TikTokOrderWebhookRenderer
from generator.config import GeneratorConfig
from generator.core.model import (
    AWAITING_SHIPMENT,
    CANCELLED,
    COMPLETED,
    DELIVERED,
    FULFILLED_BY_CHANNEL,
    PENDING_PAYMENT,
    SHIPPED,
    Order,
    OrderLine,
)
from generator.core.storefronts import CURRENCY_DECIMALS

API_STATUS = {
    PENDING_PAYMENT: "UNPAID",
    AWAITING_SHIPMENT: "AWAITING_SHIPMENT",
    SHIPPED: "IN_TRANSIT",
    DELIVERED: "DELIVERED",
    COMPLETED: "COMPLETED",
    CANCELLED: "CANCELLED",
}


class TikTokOrdersRenderer:
    """Render a TikTok Shop Order at a point in time as a Batch API response."""

    channel = "tiktok_shop"
    dataset = "orders"
    storage = OBJECT_STORE

    def __init__(self, config: GeneratorConfig) -> None:
        """Use the seed and Order id rule shared with the webhook renderer."""
        self.seed = config.seed
        self.webhook = TikTokOrderWebhookRenderer(config)

    def line_id(self, line: OrderLine) -> str:
        """Generate a stable Order Line id from the core id, not from the export date."""
        return "7" + native_digits(self.seed, "tiktok_line", line.order_line_id, 18)

    def line_keys(self, order: Order) -> list[dict[str, Any]]:
        """Map core ids to TikTok Shop Order and Order Line ids."""
        order_id = self.webhook.tiktok_order_id(order)
        return [
            {
                "order_line_id": line.order_line_id,
                "channel_order_id": order_id,
                "channel_line_id": self.line_id(line),
            }
            for line in order.lines
        ]

    def render(self, order: Order, as_of: datetime, export_day: int) -> RenderedRecord:
        """Render a Get Order Detail snapshot; `export_day` only decides the path."""
        del export_day
        status_change = order.status_at(as_of)
        order_id = self.webhook.tiktok_order_id(order)
        decimals = CURRENCY_DECIMALS[order.currency]
        gross = sum(line.unit_price * line.quantity for line in order.lines)
        discount = sum(line.unit_discount * line.quantity for line in order.lines)

        items = []
        for line in order.lines:
            sale_price = line.unit_price - line.unit_discount
            items.append(
                {
                    "id": self.line_id(line),
                    "sku_id": "7" + native_digits(self.seed, "tiktok_sku", line.sku_id, 18),
                    "product_id": "7" + native_digits(self.seed, "tiktok_product", line.sku_id, 18),
                    "seller_sku": line.sku_id,
                    "product_name": f"ACME {line.sku_id}",
                    "quantity": line.quantity,
                    "original_price": money(line.unit_price, decimals),
                    "sale_price": money(sale_price, decimals),
                    "seller_discount": money(line.unit_discount, decimals),
                    "platform_discount": money(0, decimals),
                    "display_status": API_STATUS[status_change.status],
                }
            )

        native_order: dict[str, Any] = {
            "id": order_id,
            "status": API_STATUS[status_change.status],
            "user_id": "7" + native_digits(self.seed, "tiktok_buyer", order.buyer.buyer_id, 18),
            "create_time": int(order.created_at.timestamp()),
            "update_time": int(status_change.at.timestamp()),
            "fulfillment_type": (
                "TIKTOK" if order.fulfillment == FULFILLED_BY_CHANNEL else "SELLER"
            ),
            "payment": {
                "currency": order.currency,
                "sub_total": money(gross, decimals),
                "seller_discount": money(discount, decimals),
                "platform_discount": money(0, decimals),
                "total_amount": money(gross - discount, decimals),
            },
            "line_items": items,
        }
        if status_change.status != PENDING_PAYMENT:
            native_order["recipient_address"] = {
                "city": order.buyer.city,
                "postal_code": order.buyer.postal_code,
                "region_code": order.buyer.country,
            }

        return RenderedRecord(
            key=order_id,
            record={"code": 0, "data": {"orders": [native_order]}},
        )
