"""Renderer đơn hàng Walmart: một dòng bảng `walmart.orders` trong PostgreSQL.

Mỗi dòng gồm ba cột, mô phỏng một hệ thống khác đã kéo dữ liệu từ Walmart về:
- `purchase_order_id`: khóa đơn của Walmart.
- `raw_payload`: JSON đơn hàng đúng như API "Get all orders" trả về (cột JSONB).
- `fetched_at`: lúc hệ thống đó kéo bản ghi, cũng là cột dùng để chọn bản mới nhất khi dedup.

Nguồn field: docs/edai-1/reference/channel_sources.md, mục 4 (mức Cao). Giả định của dự án:
`shippingInfo.estimatedDeliveryDate`, `trackingInfo` bên trong `orderLineStatus`,
và giá trong `charges` là giá đã trừ giảm giá (Walmart không tách dòng giảm giá).

Walmart có trạng thái `Delivered` kèm `statusDate`, nên khác Amazon, Seller thấy
được ngày giao thật.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from generator.channels.base import (
    POSTGRES,
    RenderedRecord,
    epoch_millis,
    iso_z,
    native_code,
    native_digits,
)
from generator.config import GeneratorConfig
from generator.core.model import (
    AWAITING_SHIPMENT,
    CANCELLED,
    COMPLETED,
    DELIVERED,
    PENDING_PAYMENT,
    SHIPPED,
    Order,
)
from generator.core.storefronts import CURRENCY_DECIMALS

STATUS = {
    PENDING_PAYMENT: "Created",
    AWAITING_SHIPMENT: "Acknowledged",
    SHIPPED: "Shipped",
    DELIVERED: "Delivered",
    COMPLETED: "Delivered",
    CANCELLED: "Cancelled",
}
CARRIERS = ("UPS", "FedEx", "USPS")


class WalmartOrdersRenderer:
    """Vẽ Order của Storefront Walmart US thành dòng của bảng `walmart.orders`."""

    channel = "walmart"
    dataset = "orders"
    storage = POSTGRES

    def __init__(self, config: GeneratorConfig) -> None:
        """Ghi nhớ seed để sinh mã gốc cố định."""
        self.seed = config.seed

    def purchase_order_id(self, order: Order) -> str:
        """Mã đơn 13 chữ số, không bắt đầu bằng 0."""
        return "1" + native_digits(self.seed, "walmart_po", order.order_id, 12)

    def tracking_number(self, carrier: str, po_id: str) -> str:
        """Mã vận đơn theo định dạng của từng hãng.

        UPS: `1Z` + 16 ký tự chữ và số. FedEx: 12 chữ số. USPS: `94` + 20 chữ số (22 số).
        """
        # Hàm băm chỉ cho 64 bit (khoảng 12 ký tự chữ và số, hoặc 19 chữ số), nên mã dài
        # được ghép từ hai nửa băm với `kind` khác nhau.
        if carrier == "UPS":
            return (
                "1Z"
                + native_code(self.seed, "tracking", po_id, 8)
                + native_code(self.seed, "tracking_2", po_id, 8)
            )
        if carrier == "FedEx":
            return native_digits(self.seed, "tracking", po_id, 12)
        return (
            "94"
            + native_digits(self.seed, "tracking", po_id, 10)
            + native_digits(self.seed, "tracking_2", po_id, 10)
        )

    def render(self, order: Order, as_of: datetime, export_day: int) -> RenderedRecord:
        """Vẽ `order` tại `as_of`. Mọi dòng của đơn cùng trạng thái (đơn giản hóa)."""
        change = order.status_at(as_of)
        decimals = CURRENCY_DECIMALS[order.currency]
        po_id = self.purchase_order_id(order)
        buyer = order.buyer
        status = STATUS[change.status]

        lines = []
        for line in order.lines:
            line_status: dict[str, Any] = {
                "status": status,
                "statusQuantity": {"unitOfMeasurement": "EACH", "amount": str(line.quantity)},
            }
            if order.shipped_at is not None and order.shipped_at <= as_of:
                carrier = CARRIERS[int(native_digits(self.seed, "carrier", po_id, 1)) % 3]
                line_status["trackingInfo"] = {
                    "shipDateTime": epoch_millis(order.shipped_at),
                    "carrierName": {"carrier": carrier},
                    "trackingNumber": self.tracking_number(carrier, po_id),
                }
            lines.append(
                {
                    "lineNumber": str(line.line_number),
                    "item": {"productName": f"ACME {line.sku_id}", "sku": line.sku_id},
                    "charges": {
                        "charge": [
                            {
                                "chargeType": "PRODUCT",
                                "chargeName": "ItemPrice",
                                "chargeAmount": {
                                    "currency": order.currency,
                                    "amount": round(
                                        (line.unit_price - line.unit_discount) * line.quantity,
                                        decimals,
                                    ),
                                },
                            }
                        ]
                    },
                    "orderLineQuantity": {
                        "unitOfMeasurement": "EACH",
                        "amount": str(line.quantity),
                    },
                    "statusDate": epoch_millis(change.at),
                    "orderLineStatuses": {"orderLineStatus": [line_status]},
                }
            )

        payload = {
            "purchaseOrderId": po_id,
            "customerOrderId": native_digits(self.seed, "walmart_co", order.order_id, 13),
            "customerEmailId": native_code(self.seed, "walmart_mail", buyer.buyer_id, 12).lower()
            + "@relay.walmart.com",
            "orderDate": epoch_millis(order.created_at),
            "buyerId": native_code(self.seed, "walmart_buyer", buyer.buyer_id, 12).lower(),
            "isGuest": buyer.is_guest,
            "orderType": "REGULAR",
            "shippingInfo": {
                "estimatedDeliveryDate": epoch_millis(order.promised_delivery[1]),
                "postalAddress": {
                    "city": buyer.city,
                    "postalCode": buyer.postal_code,
                    "country": "USA",
                },
            },
            "orderLines": {"orderLine": lines},
        }
        row = {"purchase_order_id": po_id, "raw_payload": payload, "fetched_at": iso_z(as_of)}
        return RenderedRecord(key=po_id, record=row)

    def line_keys(self, order: Order) -> list[dict[str, Any]]:
        """Ánh xạ Order Line chuẩn sang `purchaseOrderId` và `lineNumber`."""
        po_id = self.purchase_order_id(order)
        return [
            {
                "order_line_id": line.order_line_id,
                "channel_order_id": po_id,
                "channel_line_id": str(line.line_number),
            }
            for line in order.lines
        ]
