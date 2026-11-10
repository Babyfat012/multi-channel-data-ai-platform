"""Generate the Seller's SKU catalog and each SKU's (Zipf) popularity.

SKU skew: SKUs are ranked randomly, and the SKU of rank r has weight r^(-s)
with s = `sku_zipf_exponent`. With 2,000 SKUs and s = 0.8, the rank-1 SKU takes
about 5% of Order Lines. Ranks are shuffled, so best sellers are scattered
across all categories instead of piling into the first one.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from generator.config import GeneratorConfig, PrimeDayConfig
from generator.core.model import Sku
from generator.core.rngs import rng_for
from generator.core.storefronts import CATEGORIES

# Standard deviation of SKU Defect Propensity on the logit scale.
# 0.5 means a "bad" SKU (+1 standard deviation) has about 1.65 times higher Return odds.
DEFECT_PROPENSITY_STD = 0.5

# Weight multiplier for deal SKUs on sale days (covariate drift).
DEAL_WEIGHT_MULTIPLIER = 5.0


@dataclass(frozen=True)
class Catalog:
    """The SKU catalog with the probability of each SKU being picked for an Order Line.

    `popularity[i]` is the probability of picking `skus[i]`; they sum to 1.
    `skus_by_id` looks up a SKU by `sku_id`.
    `sale_popularity` is the replacement distribution used on Sale Event days.
    """

    skus: list[Sku]
    popularity: np.ndarray
    skus_by_id: dict[str, Sku]
    sale_popularity: np.ndarray


def build_catalog(config: GeneratorConfig) -> Catalog:
    """Generate `n_skus` SKUs split by category, with Zipf popularity."""
    rng = rng_for(config.seed, "catalog")
    n = config.scale.n_skus

    # Split the SKU count by each category's `sku_share`; the remainder goes to the last category.
    counts = [int(n * c.sku_share) for c in CATEGORIES]
    counts[-1] += n - sum(counts)

    skus: list[Sku] = []
    for category, count in zip(CATEGORIES, counts):
        low, high = category.price_range_usd
        prices = np.round(rng.uniform(low, high, size=count), 2)
        margins = rng.uniform(0.35, 0.6, size=count)
        defects = rng.normal(0.0, DEFECT_PROPENSITY_STD, size=count)
        for i in range(count):
            number = len(skus) + 1
            skus.append(
                Sku(
                    sku_id=f"{category.code}-{number:05d}",
                    name=f"{category.name.title()} item {number}",
                    category=category.name,
                    base_price_usd=float(prices[i]),
                    unit_cost_usd=round(float(prices[i] * (1 - margins[i])), 2),
                    defect_propensity=float(defects[i]),
                )
            )

    popularity = zipf_popularity(n, config.scale.sku_zipf_exponent, rng)
    return Catalog(
        skus=skus,
        popularity=popularity,
        skus_by_id={s.sku_id: s for s in skus},
        sale_popularity=sale_popularity(popularity, config.drift.prime_day, rng),
    )


def sale_popularity(
    popularity: np.ndarray, sale: PrimeDayConfig | None, rng: np.random.Generator
) -> np.ndarray:
    """Popularity on sale days: deal SKU weights are multiplied, then renormalized.

    Without a Sale Event, return `popularity` itself, i.e. sale days do not change
    the SKU distribution. The deal SKU set is drawn once from the seed, so it is fixed across days.
    """
    if sale is None:
        return popularity
    deals = rng.choice(len(popularity), size=sale.deal_skus, replace=False)
    weights = popularity.copy()
    weights[deals] *= DEAL_WEIGHT_MULTIPLIER
    return weights / weights.sum()


def zipf_popularity(n: int, exponent: float, rng: np.random.Generator) -> np.ndarray:
    """Return a Zipf probability array of length `n`, with shuffled ranks.

    E.g. n = 3, exponent = 1: rank weights are 1, 1/2, 1/3, normalized
    to 0.55 / 0.27 / 0.18, then shuffled so rank 1 lands on a random SKU.
    """
    ranks = np.arange(1, n + 1, dtype=float)
    weights = ranks ** (-exponent)
    weights /= weights.sum()
    return rng.permutation(weights)
