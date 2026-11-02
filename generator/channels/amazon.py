"""Renderer đơn hàng Amazon: Orders v0 trước ngày chuyển, Orders 2026-01-01 từ ngày chuyển.

Đây là chỗ tạo ra **schema evolution**. Ngày export nhỏ hơn
`amazon_orders_api_switch_day` thì ghi theo v0, còn lại ghi theo 2026-01-01.
Năm kiểu thay đổi generator mô phỏng:

| Kiểu | v0 | 2026-01-01 |
|---|---|---|
| Đổi tên | `AmazonOrderId`, `PurchaseDate`, `LastUpdateDate`, `OrderTotal` | `orderId`, `createdTime`, `lastUpdatedTime`, `proceeds.grandTotal` (lồng thêm một cấp) |
| Đổi cấu trúc và cách viết giá trị | `OrderStatus: "Shipped"` | `fulfillment.status: "SHIPPED"` |
| Field mới | không có | không có — mọi field dạng 2026 đều truy được về một field v0 (đổi tên, lồng cấp, hoặc đổi kiểu) |
| Field bỏ đi | `NumberOfItemsShipped`, `NumberOfItemsUnshipped`, `IsBusinessOrder`, `IsReplacementOrder`, `OrderItems[].QuantityShipped` | không có (đã bỏ) → partition 2026-01-01 bị null ở 5 khoá này |
| Đổi kiểu | `IsPrime` (bool) | `programs` (mảng) → không mất thông tin (cả hai dạng đều cho biết đơn có Prime hay không), nhưng khi gộp schema thì **cả hai cột đều null**: cột `is_prime` null ở 81 ngày 2026, cột `programs` null ở 99 ngày v0 |

Nguồn field: xem docs/edai-1/reference/channel_sources.md, mục 2. Tên field cấp đơn ở trên là mức Cao.
Tên các field con do dự án tự đặt (giả định) gồm: `fulfillment.fulfilledBy`,
`salesChannel.marketplaceId`, `proceeds.grandTotal`, `orderItems[].price`.
Ngoài ra, v0 thật tách Order Item sang lời gọi `getOrderItems`, còn ở đây
`OrderItems` được gộp vào cùng bản ghi cho đơn giản (một dòng JSON mỗi Order).

Amazon không có trạng thái "đã giao": đơn đã gửi, đã giao hay đã hoàn tất
đều hiện là `Shipped`. Ngày giao chỉ có ở dạng dự kiến
(`EarliestDeliveryDate`/`LatestDeliveryDate`) và chỉ với đơn Seller tự giao (MFN).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from generator.channels.base import (
    OBJECT_STORE,
    RenderedRecord,
    iso_z,
    money,
    native_code,
    native_digits,
)
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
    StatusChange,
)
from generator.core.storefronts import CURRENCY_DECIMALS

SCHEMA_V0 = "orders-v0"
SCHEMA_2026 = "orders-2026-01-01"

# Mã Marketplace chính thức của Amazon và tên Sales Channel tương ứng.
MARKETPLACES = {
    "amazon_us": ("ATVPDKIKX0DER", "Amazon.com"),
    "amazon_uk": ("A1F83G8C2ARO7P", "Amazon.co.uk"),
    "amazon_de": ("A1PA6795UKMFR9", "Amazon.de"),
}

STATUS_V0 = {
    PENDING_PAYMENT: "Pending",
    AWAITING_SHIPMENT: "Unshipped",
    SHIPPED: "Shipped",
    DELIVERED: "Shipped",
    COMPLETED: "Shipped",
    CANCELLED: "Canceled",
}
STATUS_2026 = {
    PENDING_PAYMENT: "PENDING",
    AWAITING_SHIPMENT: "UNSHIPPED",
    SHIPPED: "SHIPPED",
    DELIVERED: "SHIPPED",
    COMPLETED: "SHIPPED",
    CANCELLED: "CANCELLED",
}


class AmazonOrdersRenderer:
    """Vẽ Order của các Storefront Amazon thành bản ghi Orders API."""

    channel = "amazon"
    dataset = "orders"
    storage = OBJECT_STORE

    def __init__(self, config: GeneratorConfig) -> None:
        """Ghi nhớ seed (để sinh mã gốc) và ngày chuyển phiên bản schema."""
        self.seed = config.seed
        self.switch_day = config.offline_problems.amazon_orders_api_switch_day

    def schema_for(self, export_day: int) -> str:
        """Phiên bản schema dùng cho một ngày export."""
        return SCHEMA_V0 if export_day < self.switch_day else SCHEMA_2026

    def amazon_order_id(self, order: Order) -> str:
        """Mã đơn Amazon dạng 3-7-7, ví dụ `113-4829175-0038162`."""
        prefix = 111 + int(native_digits(self.seed, "amazon_prefix", order.order_id, 1))
        middle = native_digits(self.seed, "amazon_order_mid", order.order_id, 7)
        tail = native_digits(self.seed, "amazon_order_tail", order.order_id, 7)
        return f"{prefix}-{middle}-{tail}"

    def order_item_id(self, line: OrderLine) -> str:
        """Mã Order Item 14 chữ số của Amazon."""
        return native_digits(self.seed, "amazon_item", line.order_line_id, 14)

    def asin(self, sku_id: str) -> str:
        """ASIN của SKU, dạng `B0` + 8 ký tự. Một SKU có cùng ASIN ở mọi Marketplace."""
        return "B0" + native_code(self.seed, "asin", sku_id, 8)

    def render(self, order: Order, as_of: datetime, export_day: int) -> RenderedRecord:
        """Vẽ `order` tại thời điểm `as_of` theo schema của ngày `export_day`."""
        if self.schema_for(export_day) == SCHEMA_V0:
            record = self._render_v0(order, as_of)
            return RenderedRecord(key=record["AmazonOrderId"], record=record)
        record = self._render_2026(order, as_of)
        return RenderedRecord(key=record["orderId"], record=record)

    def line_keys(self, order: Order) -> list[dict[str, Any]]:
        """Ánh xạ Order Line chuẩn sang `AmazonOrderId` và `OrderItemId`."""
        amazon_id = self.amazon_order_id(order)
        return [
            {
                "order_line_id": line.order_line_id,
                "channel_order_id": amazon_id,
                "channel_line_id": self.order_item_id(line),
            }
            for line in order.lines
        ]

    # --- Hai phiên bản schema ---------------------------------------------

    def _render_v0(self, order: Order, as_of: datetime) -> dict[str, Any]:
        """Bản ghi theo model Orders v0 (tên field kiểu PascalCase)."""
        change = order.status_at(as_of)
        decimals = CURRENCY_DECIMALS[order.currency]
        marketplace_id, sales_channel = MARKETPLACES[order.storefront_id]
        shipped_qty = _shipped_quantity(order, change.status)
        total_qty = sum(line.quantity for line in order.lines)

        record: dict[str, Any] = {
            "AmazonOrderId": self.amazon_order_id(order),
            "PurchaseDate": iso_z(order.created_at),
            "LastUpdateDate": iso_z(change.at),
            "OrderStatus": STATUS_V0[change.status],
            "FulfillmentChannel": "AFN" if order.fulfillment == FULFILLED_BY_CHANNEL else "MFN",
            "SalesChannel": sales_channel,
            "MarketplaceId": marketplace_id,
            "OrderTotal": {
                "CurrencyCode": order.currency,
                "Amount": money(_order_total(order), decimals),
            },
            "NumberOfItemsShipped": shipped_qty,
            "NumberOfItemsUnshipped": total_qty - shipped_qty if change.status != CANCELLED else 0,
            "IsPrime": order.is_prime,
            "IsBusinessOrder": False,
            "IsReplacementOrder": False,
            "ShippingAddress": {
                "City": order.buyer.city,
                "PostalCode": order.buyer.postal_code,
                "CountryCode": order.buyer.country,
            },
            "OrderItems": [
                {
                    "OrderItemId": self.order_item_id(line),
                    "ASIN": self.asin(line.sku_id),
                    "SellerSKU": line.sku_id,
                    "QuantityOrdered": line.quantity,
                    "QuantityShipped": line.quantity if shipped_qty else 0,
                    "ItemPrice": {
                        "CurrencyCode": order.currency,
                        "Amount": money(line.unit_price * line.quantity, decimals),
                    },
                    "PromotionDiscount": {
                        "CurrencyCode": order.currency,
                        "Amount": money(line.unit_discount * line.quantity, decimals),
                    },
                }
                for line in order.lines
            ],
        }
        # Chỉ đơn Seller tự giao (MFN) có ngày giao dự kiến trong Orders.
        if order.fulfillment != FULFILLED_BY_CHANNEL:
            record["EarliestDeliveryDate"] = iso_z(order.promised_delivery[0])
            record["LatestDeliveryDate"] = iso_z(order.promised_delivery[1])
        return record

    def _render_2026(self, order: Order, as_of: datetime) -> dict[str, Any]:
        """Bản ghi theo model Orders 2026-01-01 (camelCase, lồng nhiều cấp hơn)."""
        change = order.status_at(as_of)
        decimals = CURRENCY_DECIMALS[order.currency]
        marketplace_id, sales_channel = MARKETPLACES[order.storefront_id]

        fulfillment: dict[str, Any] = {
            "status": STATUS_2026[change.status],
            "fulfilledBy": "AMAZON" if order.fulfillment == FULFILLED_BY_CHANNEL else "MERCHANT",
        }
        if order.fulfillment != FULFILLED_BY_CHANNEL:
            fulfillment["deliverByWindow"] = {
                "startTime": iso_z(order.promised_delivery[0]),
                "endTime": iso_z(order.promised_delivery[1]),
            }

        return {
            "orderId": self.amazon_order_id(order),
            "createdTime": iso_z(order.created_at),
            "lastUpdatedTime": iso_z(change.at),
            "salesChannel": {"marketplaceId": marketplace_id, "channelName": sales_channel},
            "programs": ["PRIME"] if order.is_prime else [],
            "fulfillment": fulfillment,
            "proceeds": {
                "grandTotal": {
                    "currencyCode": order.currency,
                    "amount": money(_order_total(order), decimals),
                }
            },
            "shippingAddress": {
                "city": order.buyer.city,
                "postalCode": order.buyer.postal_code,
                "countryCode": order.buyer.country,
            },
            "orderItems": [
                {
                    "orderItemId": self.order_item_id(line),
                    "product": {"asin": self.asin(line.sku_id), "sellerSku": line.sku_id},
                    "quantityOrdered": line.quantity,
                    "price": {
                        "currencyCode": order.currency,
                        "amount": money(line.unit_price * line.quantity, decimals),
                    },
                    "promotion": {
                        "currencyCode": order.currency,
                        "amount": money(line.unit_discount * line.quantity, decimals),
                    },
                }
                for line in order.lines
            ],
        }


def _order_total(order: Order) -> float:
    """Tổng tiền Order sau giảm giá, theo tiền tệ gốc."""
    return sum((line.unit_price - line.unit_discount) * line.quantity for line in order.lines)


def _shipped_quantity(order: Order, status: str) -> int:
    """Số sản phẩm đã gửi: toàn bộ nếu đơn đã gửi trở đi, còn lại 0."""
    if status in (SHIPPED, DELIVERED, COMPLETED):
        return sum(line.quantity for line in order.lines)
    return 0


class AmazonOrderNotificationRenderer:
    """Vẽ một lần đổi trạng thái của Order Amazon thành notification `ORDER_CHANGE`.

    Dùng cho Stream. Dạng message theo SP-API Notifications, nhưng **chưa đối
    chiếu** với tài liệu chính thức (docs/edai-1/reference/channel_sources.md ghi notification
    Amazon là mục còn thiếu), nên toàn bộ tên field là giả định, mức Thấp.

    `EventTime` là lúc đổi trạng thái thật. `NotificationMetadata.PublishTime`
    là lúc message đến, do lớp giao hàng Stream điền, nên message trễ có
    `PublishTime` muộn hơn `EventTime`.
    """

    channel = "amazon"
    dataset = "order_notifications"

    def __init__(self, config: GeneratorConfig) -> None:
        """Dùng lại cách sinh mã gốc của renderer Orders."""
        self.orders = AmazonOrdersRenderer(config)
        self.seed = config.seed

    def notification_id(self, order: Order, change: StatusChange) -> str:
        """Mã notification dạng UUID, cố định cho một lần đổi trạng thái."""
        h = native_code(
            self.seed, "amazon_notification", f"{order.order_id}:{change.status}", 32
        ).lower()
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"

    def render_event(self, order: Order, change: StatusChange) -> RenderedRecord:
        """Vẽ lần đổi trạng thái `change` của `order`. `key` là `NotificationId`."""
        marketplace_id, _ = MARKETPLACES[order.storefront_id]
        notification_id = self.notification_id(order, change)
        record = {
            "NotificationVersion": "1.0",
            "NotificationType": "ORDER_CHANGE",
            "PayloadVersion": "1.0",
            "EventTime": iso_z(change.at),
            "Payload": {
                "OrderChangeNotification": {
                    "NotificationLevel": "OrderLevel",
                    "AmazonOrderId": self.orders.amazon_order_id(order),
                    "OrderChangeType": "OrderStatusChange",
                    "OrderChangeTrigger": {"TimeOfOrderChange": iso_z(change.at)},
                    "Summary": {
                        "MarketplaceId": marketplace_id,
                        "OrderStatus": STATUS_V0[change.status],
                        "PurchaseDate": iso_z(order.created_at),
                        "FulfillmentType": "AFN"
                        if order.fulfillment == FULFILLED_BY_CHANNEL
                        else "MFN",
                    },
                },
            },
            "NotificationMetadata": {"NotificationId": notification_id},
        }
        return RenderedRecord(key=notification_id, record=record)
