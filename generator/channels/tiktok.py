"""Renderer webhook TikTok Shop type 1 (`Order status change`), dùng cho Stream.

Mỗi lần Order đổi Order Status sau `cutoff`, TikTok Shop gửi một message.
Renderer này chỉ vẽ message đó. Việc gửi trùng và burst là của lớp giao hàng
Stream, không phải của renderer.

Envelope và `data` theo docs/edai-1/reference/channel_sources.md, mục 5 (mức Cao):
`type`, `tts_notification_id`, `shop_id`, `timestamp` (giây), `data` gồm
`order_id`, `order_status`, `is_on_hold_order`, `update_time`.

Giữ nguyên chỗ không nhất quán của TikTok Shop: hủy là `CANCEL` ở webhook
nhưng `CANCELLED` ở API, để Silver phải ánh xạ cả hai.

Giả định của dự án:
- Mã đơn 18 chữ số bắt đầu bằng `57`, và `shop_id` 13 chữ số bắt đầu bằng `7`.
- `shipped` chuẩn hiện là `IN_TRANSIT`. Không sinh `ON_HOLD` và
  `AWAITING_COLLECTION`, nên `is_on_hold_order` luôn là False.
- `tts_notification_id` băm từ mã đơn và trạng thái. Một lần đổi trạng thái
  có đúng một mã, nên bản gửi trùng giữ cùng mã và Flink dedup được.
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
    """Vẽ một lần đổi trạng thái của Order TikTok Shop thành webhook type 1."""

    channel = "tiktok_shop"
    dataset = "order_status_webhook"

    def __init__(self, config: GeneratorConfig) -> None:
        """Ghi nhớ seed để sinh mã gốc cố định."""
        self.seed = config.seed

    def tiktok_order_id(self, order: Order) -> str:
        """Mã đơn TikTok Shop 18 chữ số, ví dụ `576482910375512345`."""
        return "57" + native_digits(self.seed, "tiktok_order", order.order_id, 16)

    def shop_id(self, order: Order) -> str:
        """Mã shop 13 chữ số, cố định cho mỗi Storefront."""
        return "7" + native_digits(self.seed, "tiktok_shop", order.storefront_id, 12)

    def notification_id(self, order: Order, change: StatusChange) -> str:
        """Mã thông báo 19 chữ số, cố định cho một lần đổi trạng thái."""
        return "7" + native_digits(
            self.seed, "tts_notification", f"{order.order_id}:{change.status}", 18
        )

    def render_event(self, order: Order, change: StatusChange) -> RenderedRecord:
        """Vẽ lần đổi trạng thái `change` của `order` thành message webhook.

        `key` là `tts_notification_id`, khóa dedup của Stream TikTok Shop.
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
