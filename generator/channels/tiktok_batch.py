"""Renderer Batch cho TikTok Shop Get Order Detail 202309.

Mỗi dòng JSONL là một response `data.orders` chứa đúng một Order. Cách bọc này
giữ hình dạng API nhưng vẫn cho lớp export ghi một bản ghi trên mỗi Order.
Webhook Stream giữ renderer riêng trong `tiktok.py`; hai renderer dùng cùng mã
Order nên một Order ở Batch và Stream nối được với nhau.

Field Order/Line Item và trạng thái dựa trên `docs/edai-1/reference/channel_sources.md` mục 5.
Hai khoản giản lược của dữ liệu giả lập: `seller_discount` chứa toàn bộ giảm giá
(không tách platform discount), và một Order có một loại fulfillment cho mọi dòng.
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
    """Vẽ Order TikTok Shop ở một thời điểm thành response API Batch."""

    channel = "tiktok_shop"
    dataset = "orders"
    storage = OBJECT_STORE

    def __init__(self, config: GeneratorConfig) -> None:
        """Dùng seed và quy tắc mã Order chung với renderer webhook."""
        self.seed = config.seed
        self.webhook = TikTokOrderWebhookRenderer(config)

    def line_id(self, line: OrderLine) -> str:
        """Sinh mã Order Line ổn định từ mã lõi, không từ ngày export."""
        return "7" + native_digits(self.seed, "tiktok_line", line.order_line_id, 18)

    def line_keys(self, order: Order) -> list[dict[str, Any]]:
        """Nối ID lõi với mã Order và Order Line của TikTok Shop."""
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
        """Vẽ một snapshot Get Order Detail; `export_day` chỉ quyết định đường dẫn."""
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
