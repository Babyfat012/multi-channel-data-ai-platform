"""Renderer Batch cho Shopify REST Admin Order.

Một dòng JSONL mô phỏng response `GET /orders/{id}.json`, gồm `order` và mảng
`line_items`. REST Admin API hiện là legacy; chọn dạng này vì `docs/edai-1/reference/channel_sources.md`
đã ghi schema và cả hai cách lưu Order đều dùng cùng dữ liệu lõi. Không gắn
`delivered_at`: Order resource chỉ cho biết fulfillment, không cho ngày giao thật.
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
    """Vẽ một snapshot Order Shopify với ID ổn định qua mọi Export Date."""

    channel = "shopify"
    dataset = "orders"
    storage = OBJECT_STORE

    def __init__(self, config: GeneratorConfig) -> None:
        """Ghi seed để sinh ID dạng số như API Shopify."""
        self.seed = config.seed

    def order_id(self, order: Order) -> int:
        """Sinh ID Order dạng số cố định cho một Order lõi."""
        return int("5" + native_digits(self.seed, "shopify_order", order.order_id, 12))

    def line_id(self, line: OrderLine) -> int:
        """Sinh ID Line Item dạng số cố định cho một Order Line lõi."""
        return int("6" + native_digits(self.seed, "shopify_line", line.order_line_id, 12))

    def line_keys(self, order: Order) -> list[dict[str, Any]]:
        """Nối ID lõi với ID Order/Line Item Shopify."""
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
        """Vẽ Order tại `as_of`; ngày export được lớp Batch ghi vào đường dẫn."""
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
