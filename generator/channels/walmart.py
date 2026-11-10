"""Walmart order renderer: one row of table `walmart.orders` in PostgreSQL.

Each row has three columns, simulating another system that pulled the data from Walmart:
- `purchase_order_id`: Walmart's order key.
- `raw_payload`: the order JSON exactly as the "Get all orders" API returns it (JSONB column).
- `fetched_at`: when that system pulled the record, also the column used to pick the latest version when deduping.

Field source: docs/data-platform/reference/channel_sources.md, section 4 (High level). Project assumptions:
`shippingInfo.estimatedDeliveryDate`, `trackingInfo` inside `orderLineStatus`,
and prices in `charges` are net of discounts (Walmart does not split out a discount line).

Walmart has a `Delivered` status with `statusDate`, so unlike Amazon, the Seller can see
the actual delivery date.
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
    """Render Walmart US Storefront Orders as rows of table `walmart.orders`."""

    channel = "walmart"
    dataset = "orders"
    storage = POSTGRES

    def __init__(self, config: GeneratorConfig) -> None:
        """Remember the seed to generate fixed native ids."""
        self.seed = config.seed

    def purchase_order_id(self, order: Order) -> str:
        """13-digit order id, not starting with 0."""
        return "1" + native_digits(self.seed, "walmart_po", order.order_id, 12)

    def tracking_number(self, carrier: str, po_id: str) -> str:
        """Tracking number in each carrier's format.

        UPS: `1Z` + 16 alphanumeric characters. FedEx: 12 digits. USPS: `94` + 20 digits (22 digits).
        """
        # A hash gives only 64 bits (about 12 alphanumeric characters, or 19 digits), so long ids
        # are built from two hash halves with different `kind`s.
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
        """Render `order` at `as_of`. All lines of an order share one status (simplification)."""
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
        """Map canonical Order Lines to `purchaseOrderId` and `lineNumber`."""
        po_id = self.purchase_order_id(order)
        return [
            {
                "order_line_id": line.order_line_id,
                "channel_order_id": po_id,
                "channel_line_id": str(line.line_number),
            }
            for line in order.lines
        ]
