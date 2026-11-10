"""The "truth" objects the simulation core generates.

The core does not know what Channels look like. It only knows:
- When an Order was created, in which Storefront, and which Order Lines it has.
- When the Order went through the canonical Order Statuses (`timeline`).
- Which Order Lines were returned, when, and what the true label is.

Each Channel's renderer reads these objects and "draws" them in the native shape.
The most important function for renderers is `Order.status_at(t)`: the Order's status
at any point in time, so the same Order can be exported at many
different times (old export, new export, snapshot at the Cutoff).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

# Canonical Order Statuses, in progression order (see docs/shared/domain.md, section 4).
PENDING_PAYMENT = "pending_payment"
AWAITING_SHIPMENT = "awaiting_shipment"
SHIPPED = "shipped"
DELIVERED = "delivered"
COMPLETED = "completed"
CANCELLED = "cancelled"

# Canonical Fulfillment Method: who delivers.
FULFILLED_BY_CHANNEL = "channel"  # Amazon FBA, Walmart WFS, ...
FULFILLED_BY_SELLER = "seller"  # Amazon MFN, Seller-fulfilled


@dataclass(slots=True)
class Buyer:
    """The buyer, fully known to the core. Renderers expose only what the Channel actually exposes."""

    buyer_id: str
    country: str
    city: str
    postal_code: str
    is_guest: bool


@dataclass(slots=True)
class Sku:
    """A SKU in the Seller's catalog.

    `defect_propensity` is the hidden "defectiveness": added directly to the Return probability logit.
    It is never written to Channel data and can only be inferred from Return history.
    """

    sku_id: str
    name: str
    category: str
    base_price_usd: float
    unit_cost_usd: float
    defect_propensity: float


@dataclass(slots=True)
class OrderLine:
    """A product line in an Order.

    Amounts (`unit_price`, `unit_discount`) are in the Storefront's original currency.
    `return_requested_at` is None if the line is not returned.
    `label` is None when never delivered (Order cancelled), otherwise 0 or 1.
    """

    order_line_id: str
    line_number: int
    sku_id: str
    quantity: int
    unit_price: float
    unit_discount: float
    return_requested_at: datetime | None = None
    label: int | None = None


@dataclass(slots=True)
class StatusChange:
    """The Order moves to `status` at time `at`."""

    at: datetime
    status: str


@dataclass(slots=True)
class Order:
    """An Order and its whole lifecycle.

    `timeline` is always time-ordered and starts with `pending_payment`.
    `promised_delivery` is the estimated delivery window the Channel reports at order time,
    `delivered_at` is the actual delivery date (None if cancelled). The two differ
    because Amazon only shows the Seller the estimated date (see generator-design.md, section 4).
    """

    order_id: str
    storefront_id: str
    currency: str
    buyer: Buyer
    created_at: datetime
    fulfillment: str
    is_prime: bool
    promised_delivery: tuple[datetime, datetime]
    delivered_at: datetime | None
    timeline: list[StatusChange] = field(default_factory=list)
    lines: list[OrderLine] = field(default_factory=list)

    def status_at(self, t: datetime) -> StatusChange:
        """Return the latest status change up to time `t`.

        Example: an Order created on day 1 and delivered on day 5. Asking for day 3 gives `shipped`.
        Calling with `t` before the Order is created is an error, since the Order does not exist yet.
        """
        if t < self.created_at:
            raise ValueError(f"Order {self.order_id} does not exist yet at {t}")
        current = self.timeline[0]
        for change in self.timeline[1:]:
            if change.at > t:
                break
            current = change
        return current

    @property
    def shipped_at(self) -> datetime | None:
        """Time handed to the carrier, None if never shipped."""
        return next((c.at for c in self.timeline if c.status == SHIPPED), None)
