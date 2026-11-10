"""Renderer for TikTok Shop webhook type 1 (`Order status change`), used for the Stream.

Each time an Order changes Order Status after `cutoff`, TikTok Shop sends a message.
This renderer only draws that message. Duplicate sends and bursts belong to the Stream
delivery layer, not to the renderer.

Envelope and `data` follow docs/data-platform/reference/channel_sources.md, section 5 (High level):
`type`, `tts_notification_id`, `shop_id`, `timestamp` (seconds), `data` with
`order_id`, `order_status`, `is_on_hold_order`, `update_time`.

Keeps TikTok Shop's inconsistency: cancellation is `CANCEL` in the webhook
but `CANCELLED` in the API, so Silver must map both.

Project assumptions:
- Order ids are 18 digits starting with `57`, and `shop_id` is 13 digits starting with `7`.
- Canonical `shipped` shows as `IN_TRANSIT`. `ON_HOLD` and
  `AWAITING_COLLECTION` are not generated, so `is_on_hold_order` is always False.
- `tts_notification_id` is hashed from the order id and status. One status change
  has exactly one id, so duplicate sends keep the same id and Flink can dedup them.
"""

from __future__ import annotations

from typing import Any

from generator.channels.base import RenderedRecord, native_digits
from generator.config import GeneratorConfig
from generator.core.model import (
    AWAITING_SHIPMENT,
    CANCELLED,
    COMPLETED,
    DELIVERED,
    PENDING_PAYMENT,
    SHIPPED,
    Order,
    StatusChange,
)

ORDER_STATUS_CHANGE = 1

WEBHOOK_STATUS = {
    PENDING_PAYMENT: "UNPAID",
    AWAITING_SHIPMENT: "AWAITING_SHIPMENT",
    SHIPPED: "IN_TRANSIT",
    DELIVERED: "DELIVERED",
    COMPLETED: "COMPLETED",
    CANCELLED: "CANCEL",
}


class TikTokOrderWebhookRenderer:
    """Render one TikTok Shop Order status change as a type 1 webhook."""

    channel = "tiktok_shop"
    dataset = "order_status_webhook"

    def __init__(self, config: GeneratorConfig) -> None:
        """Remember the seed to generate fixed native ids."""
        self.seed = config.seed

    def tiktok_order_id(self, order: Order) -> str:
        """18-digit TikTok Shop order id, e.g. `576482910375512345`."""
        return "57" + native_digits(self.seed, "tiktok_order", order.order_id, 16)

    def shop_id(self, order: Order) -> str:
        """13-digit shop id, fixed per Storefront."""
        return "7" + native_digits(self.seed, "tiktok_shop", order.storefront_id, 12)

    def notification_id(self, order: Order, change: StatusChange) -> str:
        """19-digit notification id, fixed per status change."""
        return "7" + native_digits(
            self.seed, "tts_notification", f"{order.order_id}:{change.status}", 18
        )

    def render_event(self, order: Order, change: StatusChange) -> RenderedRecord:
        """Render status change `change` of `order` as a webhook message.

        `key` is `tts_notification_id`, the dedup key of the TikTok Shop Stream.
        """
        update_time = int(change.at.timestamp())
        notification_id = self.notification_id(order, change)
        record: dict[str, Any] = {
            "type": ORDER_STATUS_CHANGE,
            "tts_notification_id": notification_id,
            "shop_id": self.shop_id(order),
            "timestamp": update_time,
            "data": {
                "order_id": self.tiktok_order_id(order),
                "order_status": WEBHOOK_STATUS[change.status],
                "is_on_hold_order": False,
                "update_time": update_time,
            },
        }
        return RenderedRecord(key=notification_id, record=record)
