"""Amazon order renderer: Orders v0 before the switch day, Orders 2026-01-01 from the switch day on.

This is where **schema evolution** is created. Export days before
`amazon_orders_api_switch_day` are written as v0, the rest as 2026-01-01.
Five kinds of change the generator simulates:

| Kind | v0 | 2026-01-01 |
|---|---|---|
| Rename | `AmazonOrderId`, `PurchaseDate`, `LastUpdateDate`, `OrderTotal` | `orderId`, `createdTime`, `lastUpdatedTime`, `proceeds.grandTotal` (nested one level deeper) |
| Structure and value spelling change | `OrderStatus: "Shipped"` | `fulfillment.status: "SHIPPED"` |
| New field | none | none — every 2026 field traces back to a v0 field (renamed, nested, or retyped) |
| Dropped field | `NumberOfItemsShipped`, `NumberOfItemsUnshipped`, `IsBusinessOrder`, `IsReplacementOrder`, `OrderItems[].QuantityShipped` | none (dropped) → the 2026-01-01 partition is null on these 5 keys |
| Type change | `IsPrime` (bool) | `programs` (array) → no information lost (both forms tell whether the order is Prime), but after schema merge **both columns are null**: `is_prime` is null on the 81 days of 2026, `programs` is null on the 99 v0 days |

Field source: see docs/data-platform/reference/channel_sources.md, section 2. The order-level field names above are High level.
Sub-field names chosen by the project (assumptions) are: `fulfillment.fulfilledBy`,
`salesChannel.marketplaceId`, `proceeds.grandTotal`, `orderItems[].price`.
Also, the real v0 splits Order Items into a separate `getOrderItems` call, while here
`OrderItems` is folded into the same record for simplicity (one JSON row per Order).

Amazon has no "delivered" status: shipped, delivered and completed orders
all show as `Shipped`. Delivery dates only exist as estimates
(`EarliestDeliveryDate`/`LatestDeliveryDate`) and only for Seller-fulfilled orders (MFN).
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

# Amazon's official Marketplace ids and their Sales Channel names.
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
    """Render Amazon Storefront Orders as Orders API records."""

    channel = "amazon"
    dataset = "orders"
    storage = OBJECT_STORE

    def __init__(self, config: GeneratorConfig) -> None:
        """Remember the seed (to generate native ids) and the schema version switch day."""
        self.seed = config.seed
        self.switch_day = config.offline_problems.amazon_orders_api_switch_day

    def schema_for(self, export_day: int) -> str:
        """Schema version used for an export day."""
        return SCHEMA_V0 if export_day < self.switch_day else SCHEMA_2026

    def amazon_order_id(self, order: Order) -> str:
        """Amazon order id in 3-7-7 form, e.g. `113-4829175-0038162`."""
        prefix = 111 + int(native_digits(self.seed, "amazon_prefix", order.order_id, 1))
        middle = native_digits(self.seed, "amazon_order_mid", order.order_id, 7)
        tail = native_digits(self.seed, "amazon_order_tail", order.order_id, 7)
        return f"{prefix}-{middle}-{tail}"

    def order_item_id(self, line: OrderLine) -> str:
        """Amazon's 14-digit Order Item id."""
        return native_digits(self.seed, "amazon_item", line.order_line_id, 14)

    def asin(self, sku_id: str) -> str:
        """The SKU's ASIN, `B0` + 8 characters. A SKU has the same ASIN in every Marketplace."""
        return "B0" + native_code(self.seed, "asin", sku_id, 8)

    def render(self, order: Order, as_of: datetime, export_day: int) -> RenderedRecord:
        """Render `order` at time `as_of` using the schema of day `export_day`."""
        if self.schema_for(export_day) == SCHEMA_V0:
            record = self._render_v0(order, as_of)
            return RenderedRecord(key=record["AmazonOrderId"], record=record)
        record = self._render_2026(order, as_of)
        return RenderedRecord(key=record["orderId"], record=record)

    def line_keys(self, order: Order) -> list[dict[str, Any]]:
        """Map canonical Order Lines to `AmazonOrderId` and `OrderItemId`."""
        amazon_id = self.amazon_order_id(order)
        return [
            {
                "order_line_id": line.order_line_id,
                "channel_order_id": amazon_id,
                "channel_line_id": self.order_item_id(line),
            }
            for line in order.lines
        ]

    # --- The two schema versions -----------------------------------------

    def _render_v0(self, order: Order, as_of: datetime) -> dict[str, Any]:
        """Record in the Orders v0 model (PascalCase field names)."""
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
        # Only Seller-fulfilled (MFN) orders have an estimated delivery date in Orders.
        if order.fulfillment != FULFILLED_BY_CHANNEL:
            record["EarliestDeliveryDate"] = iso_z(order.promised_delivery[0])
            record["LatestDeliveryDate"] = iso_z(order.promised_delivery[1])
        return record

    def _render_2026(self, order: Order, as_of: datetime) -> dict[str, Any]:
        """Record in the Orders 2026-01-01 model (camelCase, more deeply nested)."""
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
    """Order total after discounts, in the original currency."""
    return sum((line.unit_price - line.unit_discount) * line.quantity for line in order.lines)


def _shipped_quantity(order: Order, status: str) -> int:
    """Units shipped: all of them if the order is shipped or later, otherwise 0."""
    if status in (SHIPPED, DELIVERED, COMPLETED):
        return sum(line.quantity for line in order.lines)
    return 0


class AmazonOrderNotificationRenderer:
    """Render one Amazon Order status change as an `ORDER_CHANGE` notification.

    Used for the Stream. The message shape follows SP-API Notifications, but is **not yet
    checked** against the official docs (docs/data-platform/reference/channel_sources.md lists Amazon
    notifications as a missing item), so every field name is an assumption, Low level.

    `EventTime` is when the status actually changed. `NotificationMetadata.PublishTime`
    is when the message arrives, filled in by the Stream delivery layer, so late messages have
    `PublishTime` later than `EventTime`.
    """

    channel = "amazon"
    dataset = "order_notifications"

    def __init__(self, config: GeneratorConfig) -> None:
        """Reuse the Orders renderer's native id generation."""
        self.orders = AmazonOrdersRenderer(config)
        self.seed = config.seed

    def notification_id(self, order: Order, change: StatusChange) -> str:
        """UUID-style notification id, fixed per status change."""
        h = native_code(
            self.seed, "amazon_notification", f"{order.order_id}:{change.status}", 32
        ).lower()
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"

    def render_event(self, order: Order, change: StatusChange) -> RenderedRecord:
        """Render status change `change` of `order`. `key` is `NotificationId`."""
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
