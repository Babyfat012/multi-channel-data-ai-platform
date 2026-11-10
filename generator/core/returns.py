"""Rules deciding which Order Lines are returned and how long after.

The Return probability is the logistic function of a sum of "effects" (see
generator-design.md, section 4):

    logit(p) = logit(category Return rate)          baseline
             + the SKU's defect_propensity          hidden, only inferable from history
             + Storefront shift
             + Fulfillment Method effect
             + discount, price and quantity effects

The logistic function maps this sum to a probability in (0, 1). Logits are used instead of
adding probabilities directly because adding many effects on the logit scale can never
yield a negative probability or one above 1.

Each effect is **centered**: its mean is subtracted, so averaged over all
Order Lines the effect is close to 0. That keeps each category's Return rate
close to the configured `return_rate`, while individual lines shift up or down.
Discount example: about 25% of lines get an average 17.5% discount, i.e. a mean
discount of about 4.4%. Undiscounted lines get a small minus, deeply discounted lines a large plus.

The model can learn the task because every effect (except defect_propensity) is visible
in the data at Order creation, while defect_propensity is learnable through per-SKU Return
history features. No effect uses information only available after delivery, so
there is no leakage.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta

import numpy as np

from generator.core.model import FULFILLED_BY_CHANNEL, OrderLine, Sku
from generator.core.storefronts import CATEGORIES, Storefront

# Coefficient of each effect on the logit scale.
CHANNEL_FULFILLED_SHIFT = 0.25  # Channel-fulfilled items are easier to return (prepaid label, back to the Channel's warehouse)
DISCOUNT_COEF = 1.2  # a 30% discount raises the logit by about 0.31 over the mean
LOG_PRICE_COEF = 0.25  # SKUs priced above the category midpoint are returned more often
EXTRA_QUANTITY_COEF = 0.15  # buying several at once (trying sizes) makes a Return more likely

# Means used for centering (matching how the core generates data in simulate.py).
MEAN_CHANNEL_FULFILLED = 0.6  # Channel-fulfilled line share, weighted by Storefront share
MEAN_DISCOUNT_FRACTION = 0.044
MEAN_QUANTITY = 1.2

_CATEGORY_BY_NAME = {c.name: c for c in CATEGORIES}


def return_probability(
    sku: Sku,
    storefront: Storefront,
    fulfillment: str,
    discount_fraction: float,
    quantity: int,
) -> float:
    """Compute the probability that an Order Line is returned, per the formula at the top of the file."""
    category = _CATEGORY_BY_NAME[sku.category]
    low, high = category.price_range_usd
    mid_price = (low + high) / 2

    z = _logit(category.return_rate)
    z += sku.defect_propensity
    z += storefront.return_logit_shift
    by_channel = 1.0 if fulfillment == FULFILLED_BY_CHANNEL else 0.0
    z += CHANNEL_FULFILLED_SHIFT * (by_channel - MEAN_CHANNEL_FULFILLED)
    z += DISCOUNT_COEF * (discount_fraction - MEAN_DISCOUNT_FRACTION)
    z += LOG_PRICE_COEF * math.log(sku.base_price_usd / mid_price)
    z += EXTRA_QUANTITY_COEF * (quantity - MEAN_QUANTITY)
    return 1.0 / (1.0 + math.exp(-z))


def decide_return(
    line: OrderLine,
    probability: float,
    delivered_at: datetime,
    storefront: Storefront,
    window_days: int,
    rng: np.random.Generator,
) -> None:
    """Decide whether line `line` is returned, then write the outcome and label into `line`.

    The delay from delivery to the Return request is drawn from a Gamma distribution (shape 2)
    with mean equal to the Storefront's `return_lag_mean_days`. The distribution has a long
    tail, so occasionally a Return arrives after 30 days: still a real Return, but
    labeled 0 because it falls outside the window.
    """
    if rng.random() >= probability:
        line.label = 0
        return

    lag_days = rng.gamma(shape=2.0, scale=storefront.return_lag_mean_days / 2.0)
    line.return_requested_at = delivered_at + timedelta(days=float(lag_days))
    rng.uniform(2, 7)  # Discarded draw to keep the random sequence of existing data unchanged.
    line.label = 1 if lag_days <= window_days else 0


def _logit(p: float) -> float:
    """Inverse of the logistic function: map probability `p` to the logit scale."""
    return math.log(p / (1 - p))
