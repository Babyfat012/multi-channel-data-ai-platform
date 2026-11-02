"""Renderer Batch cho Shopee `get_order_detail`.

Mỗi dòng JSONL là response `response.order_list` có một Order. `COMPLETED` của
Shopee là proxy trạng thái sau giao; nguồn không có thời điểm giao thật trong
các field đã xác minh, nên renderer không thêm `delivered_at`.

Field dựa trên `docs/edai-1/reference/channel_sources.md` mục 6. Những ID và carrier được sinh
ổn định từ seed; chúng không chứa thông tin lõi ngoài mã nối ground truth.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from generator.channels.base import OBJECT_STORE, RenderedRecord, native_code, native_digits
from generator.config import GeneratorConfig
from generator.core.model import (
    AWAITING_SHIPMENT,
    CANCELLED,
    COMPLETED,
    DELIVERED,
    PENDING_PAYMENT,
    SHIPPED,
    Order,
    OrderLine,
)

STATUS = {
    PENDING_PAYMENT: "UNPAID",
    AWAITING_SHIPMENT: "READY_TO_SHIP",
    SHIPPED: "SHIPPED",
    DELIVERED: "COMPLETED",
    COMPLETED: "COMPLETED",
    CANCELLED: "CANCELLED",
}


class ShopeeOrdersRenderer:
    """Vẽ một snapshot Order Shopee với ID ổn định qua mọi Export Date."""

    channel = "shopee"
    dataset = "orders"
    storage = OBJECT_STORE

    def __init__(self, config: GeneratorConfig) -> None:
        """Ghi seed để sinh mã gốc Shopee."""
        self.seed = config.seed

    def order_sn(self, order: Order) -> str:
        """Sinh mã Order chữ và số cố định cho một Order lõi."""
        return "VN" + native_code(self.seed, "shopee_order", order.order_id, 12)

    def line_id(self, line: OrderLine) -> str:
        """Sinh mã Order Line dạng số cố định cho một Order Line lõi."""
        return "1" + native_digits(self.seed, "shopee_line", line.order_line_id, 15)

    def line_keys(self, order: Order) -> list[dict[str, Any]]:
        """Nối ID lõi với order_sn và order_item_id Shopee."""
        order_sn = self.order_sn(order)
        return [
            {
                "order_line_id": line.order_line_id,
                "channel_order_id": order_sn,
                "channel_line_id": self.line_id(line),
            }
            for line in order.lines
        ]

    def render(self, order: Order, as_of: datetime, export_day: int) -> RenderedRecord:
        """Vẽ một Order tại `as_of`; ngày export được lớp Batch ghi vào đường dẫn."""
        del export_day
        status_change = order.status_at(as_of)
        status = status_change.status
        items = []
        for line in order.lines:
            sku_native = int("1" + native_digits(self.seed, "shopee_item", line.sku_id, 14))
            model_native = int("2" + native_digits(self.seed, "shopee_model", line.sku_id, 14))
            items.append(
                {
                    "order_item_id": self.line_id(line),
                    "item_id": sku_native,
                    "item_name": f"ACME {line.sku_id}",
                    "item_sku": line.sku_id,
                    "model_id": model_native,
                    "model_sku": line.sku_id,
                    "model_quantity_purchased": line.quantity,
                    "model_original_price": int(round(line.unit_price)),
                    "model_discounted_price": int(round(line.unit_price - line.unit_discount)),
                }
            )

        total = sum((line.unit_price - line.unit_discount) * line.quantity for line in order.lines)
        paid_at = next(
            (change.at for change in order.timeline if change.status == AWAITING_SHIPMENT), None
        )
        native_order: dict[str, Any] = {
            "order_sn": self.order_sn(order),
            "region": "VN",
            "currency": order.currency,
            "cod": False,
            "total_amount": int(round(total)),
            "order_status": STATUS[status],
            "create_time": int(order.created_at.timestamp()),
            "update_time": int(status_change.at.timestamp()),
            "pay_time": int(paid_at.timestamp()) if paid_at and paid_at <= as_of else None,
            "buyer_user_id": "1"
            + native_digits(self.seed, "shopee_buyer", order.buyer.buyer_id, 15),
            "recipient_address": {
                "city": order.buyer.city,
                "zipcode": order.buyer.postal_code,
            },
            "item_list": items,
        }
        if status in (SHIPPED, DELIVERED, COMPLETED):
            native_order["shipping_carrier"] = "SPX Express"

        return RenderedRecord(
            key=native_order["order_sn"],
            record={"response": {"order_list": [native_order]}},
        )
