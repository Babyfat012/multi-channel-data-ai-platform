"""Simulation core: generate one day's Orders along with their whole lifecycle.

Each call to `Simulator.simulate_day(day)` returns the Orders **created** on
day `day`. Each Order already carries its whole future: when it is paid, shipped,
delivered, whether it is cancelled, which lines are returned and when. The core ignores
`cutoff`. Cutting down to "what is visible as of today" is the job of the export layer and
the renderers, via `Order.status_at(t)`.

An Order's lifecycle (times measured from creation):

    pending_payment ──(a few minutes)──► awaiting_shipment ──(0.3 to 3 days)──► shipped
        ──(1 to 6 days)──► delivered ──(3 to 7 days)──► completed

About 3% of Orders are cancelled within 24 hours of payment, before shipping.
The lifecycle then stops at `cancelled` and the Order Lines have no label.

TikTok livestream (after `cutoff`): during the few livestream hours, TikTok Shop gets
`live_multiplier` times its normal Orders. The extra Orders are drawn with a separate
generator, so the day's other Orders are identical to a day without a livestream.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np

from generator.config import GeneratorConfig
from generator.core.catalog import Catalog, build_catalog
from generator.core.model import (
    AWAITING_SHIPMENT,
    CANCELLED,
    COMPLETED,
    DELIVERED,
    FULFILLED_BY_CHANNEL,
    FULFILLED_BY_SELLER,
    PENDING_PAYMENT,
    SHIPPED,
    Buyer,
    Order,
    OrderLine,
    StatusChange,
)
from generator.core.returns import decide_return, return_probability
from generator.core.rngs import rng_for, stable_int
from generator.core.storefronts import CURRENCY_DECIMALS, POSTAL_PREFIX, STOREFRONTS, Storefront

CANCEL_RATE = 0.03
DISCOUNT_RATE = 0.25  # share of Order Lines with a discount
EXTRA_LINES_MEAN = 0.45  # mean extra Order Lines, so about 1.45 lines per Order on average
MAX_LINES = 5
CITY_ZIPF_EXPONENT = 1.3  # a few large cities take most Orders (geographic skew)
BUYERS_PER_ORDER = 0.5  # buyer pool size relative to Order count; below 1 so some buyers buy again
GUEST_SHARE = {"shopify": 0.20, "walmart_us": 0.10}  # guest checkouts
DISCOUNT_RANGE = (0.05, 0.30)  # discount range on normal days
SALE_DISCOUNT_RANGE = (0.20, 0.40)  # discount range on Sale Event days


@dataclass(frozen=True)
class BuyerPool:
    """A Storefront's fixed buyer pool, generated once at init.

    Buyer i always has the same city and postal code, so the address stays the same
    each time that buyer buys again. Stored as numpy arrays to save memory.
    """

    storefront: Storefront
    city_index: np.ndarray
    postal_suffix: np.ndarray
    is_guest: np.ndarray

    def buyer(self, i: int) -> Buyer:
        """Build the `Buyer` object for the Storefront's buyer `i`."""
        city = self.storefront.cities[int(self.city_index[i])]
        return Buyer(
            buyer_id=f"{self.storefront.storefront_id}-B{i:07d}",
            country=self.storefront.country,
            city=city,
            postal_code=postal_code(self.storefront.country, city, int(self.postal_suffix[i])),
            is_guest=bool(self.is_guest[i]),
        )


# Letters used in UK inward codes (C, I, K, M, O, V excluded as easily confused).
_UK_INWARD_LETTERS = "ABDEFGHJLNPQRSTUWXYZ"


def postal_code(country: str, city: str, suffix: int) -> str:
    """Build a postal code in `country`'s format, within `city`'s area.

    `suffix` (0 to 999) distinguishes addresses within the same city. E.g. with suffix 248:
    New York `10048`, Berlin `10248`, London `SW9 2WT`, Hanoi `10248`.
    """
    prefix = POSTAL_PREFIX[city]
    if country == "US":
        return f"{prefix}{suffix % 100:02d}"
    if country == "GB":
        district = suffix % 20 + 1
        letters = _UK_INWARD_LETTERS
        h = stable_int("postal", city, suffix)
        return f"{prefix}{district} {suffix // 100}{letters[h % 20]}{letters[h // 20 % 20]}"
    # Germany and Vietnam: first 2 digits by area, then 3 digits.
    return f"{prefix}{suffix:03d}"


class Simulator:
    """Holds the fixed parts of the world (SKUs, buyers) and generates Orders per day."""

    def __init__(self, config: GeneratorConfig) -> None:
        """Build the SKU catalog and buyer pools from the config."""
        self.config = config
        self.catalog: Catalog = build_catalog(config)
        self.storefronts = [STOREFRONTS[s] for s in config.storefront_weights]
        self.storefront_weights = np.array(list(config.storefront_weights.values()))
        self.buyer_pools = {
            s.storefront_id: self._build_buyer_pool(s, w)
            for s, w in zip(self.storefronts, self.storefront_weights)
        }
        # The Sale Event only happens on the Amazon Storefronts.
        self.is_amazon = np.array([s.channel == "amazon" for s in self.storefronts])
        # Livestream burst multiplier, drawn once for the whole run.
        live = config.drift.tiktok_live
        self.live_multiplier = (
            float(rng_for(config.seed, "tiktok_live").uniform(*live.burst_multiplier))
            if live is not None
            else 1.0
        )

    def is_sale_day(self, day: int) -> bool:
        """Whether simulated day `day` is part of a Sale Event (Prime Day)."""
        sale = self.config.drift.prime_day
        return sale is not None and sale.covers(day)

    def _build_buyer_pool(self, storefront: Storefront, weight: float) -> BuyerPool:
        """Generate a Storefront's buyer pool, sized in proportion to its expected Orders."""
        rng = rng_for(self.config.seed, "buyers", storefront.storefront_id)
        expected_orders = self.config.scale.orders_per_day * self.config.scale.days_history * weight
        size = max(10, int(expected_orders * BUYERS_PER_ORDER))

        n_cities = len(storefront.cities)
        city_weights = np.arange(1, n_cities + 1, dtype=float) ** (-CITY_ZIPF_EXPONENT)
        city_weights /= city_weights.sum()

        return BuyerPool(
            storefront=storefront,
            city_index=rng.choice(n_cities, size=size, p=city_weights),
            postal_suffix=rng.integers(0, 1000, size=size),
            is_guest=rng.random(size) < GUEST_SHARE.get(storefront.storefront_id, 0.0),
        )

    def simulate_day(self, day: int) -> Iterator[Order]:
        """Generate every Order created on day `day`, in `created_at` order.

        Returns an iterator so the whole day is not held in RAM; still deterministic since each day has its own `rng`.
        The TikTok livestream day is the exception: the whole day is collected to merge the livestream Orders in order.
        """
        rng = rng_for(self.config.seed, "orders", day)
        day_start = self.config.day_start(day)
        sale_day = self.is_sale_day(day)

        # Sale day: Amazon Storefront Orders are multiplied, so the day's total Orders
        # grow by Amazon's share, and the mix across Storefronts tilts heavily toward Amazon.
        weights = self.storefront_weights
        expected = self.config.scale.orders_per_day
        if sale_day:
            weights = weights * np.where(
                self.is_amazon, self.config.drift.prime_day.order_multiplier, 1.0
            )
            expected *= weights.sum()
            weights = weights / weights.sum()

        n_orders = int(rng.poisson(expected))
        storefront_idx = rng.choice(len(self.storefronts), size=n_orders, p=weights)
        n_lines = 1 + np.minimum(rng.poisson(EXTRA_LINES_MEAN, size=n_orders), MAX_LINES - 1)
        sku_idx = rng.choice(
            len(self.catalog.skus), size=int(n_lines.sum()), p=self.catalog.popularity
        )
        if sale_day:
            # Amazon Storefront lines are redrawn from the sale-day SKU distribution.
            on_sale = self.is_amazon[np.repeat(storefront_idx, n_lines)]
            sku_idx[on_sale] = rng.choice(
                len(self.catalog.skus), size=int(on_sale.sum()), p=self.catalog.sale_popularity
            )
        created_offsets = np.sort(rng.uniform(0, 86_400, size=n_orders))

        def created_orders() -> Iterator[Order]:
            """The day's regular Orders, already ordered because `created_offsets` is sorted."""
            next_sku = 0
            for i in range(n_orders):
                storefront = self.storefronts[storefront_idx[i]]
                created_at = day_start + timedelta(seconds=float(created_offsets[i]))
                skus_of_order = sku_idx[next_sku : next_sku + n_lines[i]]
                next_sku += n_lines[i]
                yield self._make_order(
                    f"ORD-{day:03d}-{i + 1:05d}",
                    storefront,
                    created_at,
                    skus_of_order,
                    rng,
                    sale_day,
                )

        live = self.config.drift.tiktok_live
        if live is None or live.day != day:
            yield from created_orders()
            return
        orders = list(created_orders()) + self._live_orders(day, first_number=n_orders + 1)
        orders.sort(key=lambda o: o.created_at)
        yield from orders

    def _live_orders(self, day: int, first_number: int) -> list[Order]:
        """Extra TikTok Shop Orders during the livestream on day `day`.

        TikTok Shop's normal rate is `orders_per_day * share / 24` per hour.
        During the livestream, `(live_multiplier - 1)` times that rate is added, so the total rate
        is `live_multiplier` times normal.
        """
        live = self.config.drift.tiktok_live
        idx = next(
            (i for i, s in enumerate(self.storefronts) if s.storefront_id == "tiktok_shop_us"), None
        )
        if idx is None:
            return []
        storefront = self.storefronts[idx]
        rng = rng_for(self.config.seed, "tiktok_live_orders", day)
        per_hour = self.config.scale.orders_per_day * self.storefront_weights[idx] / 24
        n_orders = int(rng.poisson(per_hour * (self.live_multiplier - 1) * live.duration_hours))
        start, _ = live.window(self.config.day_start(day))
        offsets = np.sort(rng.uniform(0, live.duration_hours * 3600, size=n_orders))

        orders = []
        for i in range(n_orders):
            n_lines = 1 + min(int(rng.poisson(EXTRA_LINES_MEAN)), MAX_LINES - 1)
            skus = rng.choice(len(self.catalog.skus), size=n_lines, p=self.catalog.popularity)
            created_at = start + timedelta(seconds=float(offsets[i]))
            orders.append(
                self._make_order(
                    f"ORD-{day:03d}-{first_number + i:05d}", storefront, created_at, skus, rng
                )
            )
        return orders

    def _make_order(
        self,
        order_id: str,
        storefront: Storefront,
        created_at: datetime,
        sku_indices: np.ndarray,
        rng: np.random.Generator,
        sale_day: bool = False,
    ) -> Order:
        """Create an Order with its full lifecycle, then decide the Return for each line."""
        pool = self.buyer_pools[storefront.storefront_id]
        buyer = pool.buyer(int(rng.integers(len(pool.city_index))))

        by_channel = rng.random() < storefront.channel_fulfilled_share
        fulfillment = FULFILLED_BY_CHANNEL if by_channel else FULFILLED_BY_SELLER
        is_prime = storefront.channel == "amazon" and by_channel and rng.random() < 0.6

        # Estimated delivery date the Channel promises at order time.
        promise_days = (1, 3) if is_prime else (3, 5) if by_channel else (4, 8)
        promised = (
            created_at + timedelta(days=promise_days[0]),
            created_at + timedelta(days=promise_days[1]),
        )

        # The actual lifecycle.
        paid_at = created_at + timedelta(minutes=float(rng.exponential(10)))
        timeline = [
            StatusChange(created_at, PENDING_PAYMENT),
            StatusChange(paid_at, AWAITING_SHIPMENT),
        ]
        delivered_at = None
        if rng.random() < CANCEL_RATE:
            timeline.append(
                StatusChange(paid_at + timedelta(hours=float(rng.uniform(0, 24))), CANCELLED)
            )
        else:
            handling = rng.uniform(0.3, 1.5) if by_channel else rng.uniform(0.5, 3.0)
            transit = rng.uniform(1.0, 3.0) if by_channel else rng.uniform(2.0, 6.0)
            shipped_at = paid_at + timedelta(days=float(handling))
            delivered_at = shipped_at + timedelta(days=float(transit))
            completed_at = delivered_at + timedelta(days=float(rng.uniform(3, 7)))
            timeline += [
                StatusChange(shipped_at, SHIPPED),
                StatusChange(delivered_at, DELIVERED),
                StatusChange(completed_at, COMPLETED),
            ]

        order = Order(
            order_id=order_id,
            storefront_id=storefront.storefront_id,
            currency=storefront.currency,
            buyer=buyer,
            created_at=created_at,
            fulfillment=fulfillment,
            is_prime=is_prime,
            promised_delivery=promised,
            delivered_at=delivered_at,
            timeline=timeline,
        )
        for number, idx in enumerate(sku_indices, start=1):
            order.lines.append(self._make_line(order, number, int(idx), storefront, rng, sale_day))
        return order

    def _make_line(
        self,
        order: Order,
        line_number: int,
        sku_index: int,
        storefront: Storefront,
        rng: np.random.Generator,
        sale_day: bool = False,
    ) -> OrderLine:
        """Create an Order Line: price, discount, quantity, and the Return outcome.

        On Sale Event days, Amazon Storefront lines are discounted more often
        and more deeply. Since the discount is an input to the Return probability, the Return rate
        of those days rises too. That is the covariate drift.
        """
        sku = self.catalog.skus[sku_index]
        decimals = CURRENCY_DECIMALS[storefront.currency]
        quantity = 1 + int(min(rng.poisson(0.2), 3))
        unit_price = round(sku.base_price_usd * storefront.price_factor, decimals)
        on_sale = sale_day and storefront.channel == "amazon"
        share = self.config.drift.prime_day.discount_share if on_sale else DISCOUNT_RATE
        low, high = SALE_DISCOUNT_RANGE if on_sale else DISCOUNT_RANGE
        discount_fraction = float(rng.uniform(low, high)) if rng.random() < share else 0.0

        line = OrderLine(
            order_line_id=f"{order.order_id}-{line_number}",
            line_number=line_number,
            sku_id=sku.sku_id,
            quantity=quantity,
            unit_price=unit_price,
            unit_discount=round(unit_price * discount_fraction, decimals),
        )
        if order.delivered_at is not None:
            p = return_probability(sku, storefront, order.fulfillment, discount_fraction, quantity)
            decide_return(
                line, p, order.delivered_at, storefront, self.config.labels.window_days, rng
            )
        return line
