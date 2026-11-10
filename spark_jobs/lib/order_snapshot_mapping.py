"""Silver mapping tables (status, fulfillment, country), with no pyspark dependency.

Spark jobs look these dicts up with `create_map`: unknown keys give NULL (except country, kept as is).
Keeping them in plain Python lets `pytest` check them without Spark. `channel_status` keeps the source
text, `order_status` is the canonical status. Columns that exist only once (`fulfillment_method`,
`country`) must be normalized.
"""

from __future__ import annotations

# Canonical statuses (docs/shared/terminology.md).
PENDING_PAYMENT = "pending_payment"
AWAITING_SHIPMENT = "awaiting_shipment"
SHIPPED = "shipped"
DELIVERED = "delivered"
COMPLETED = "completed"
CANCELLED = "cancelled"

# Amazon v0 `OrderStatus`. "Shipped" covers shipped/delivered/completed: Amazon does not report
# "delivered" to the Seller.
AMAZON_V0_STATUS: dict[str, str] = {
    "Pending": PENDING_PAYMENT,
    "Unshipped": AWAITING_SHIPMENT,
    "Shipped": SHIPPED,
    "Canceled": CANCELLED,
}

# Amazon 2026 `fulfillment.status`. A separate table because "Canceled" and "CANCELLED" even differ in spelling.
AMAZON_2026_STATUS: dict[str, str] = {
    "PENDING": PENDING_PAYMENT,
    "UNSHIPPED": AWAITING_SHIPMENT,
    "SHIPPED": SHIPPED,
    "CANCELLED": CANCELLED,
}

# Walmart "Delivered" covers both delivered and completed. Pick `delivered` because it is always true:
# every completed Order has passed through delivered.
WALMART_STATUS: dict[str, str] = {
    "Created": PENDING_PAYMENT,
    "Acknowledged": AWAITING_SHIPMENT,
    "Shipped": SHIPPED,
    "Delivered": DELIVERED,
    "Cancelled": CANCELLED,
}

# Who ships the Order.
BY_CHANNEL = "channel"  # Channel ships (Amazon FBA / AFN)
BY_SELLER = "seller"  # Seller ships (Amazon FBM / MFN)

# v0 uses AFN/MFN, 2026 uses AMAZON/MERCHANT; the four values do not overlap, so they share one table.
AMAZON_FULFILLMENT: dict[str, str] = {
    "AFN": BY_CHANNEL,
    "AMAZON": BY_CHANNEL,
    "MFN": BY_SELLER,
    "MERCHANT": BY_SELLER,
}

# Walmart writes "USA"; Amazon already uses 2-letter codes.
COUNTRY_ALIASES: dict[str, str] = {"USA": "US"}
