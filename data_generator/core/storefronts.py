"""Constants describing the 7 Storefronts and 8 product categories of the Seller ACME.

This is the fixed "geography" of the simulated world. Numbers that change often
(Order share per Storefront) live in the config, while things tied to
the nature of a Storefront (currency, cities, fulfillment) live here.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Storefront:
    """A place where the Seller sells: a Channel plus a Marketplace if the Channel has one.

    Attributes:
        storefront_id: canonical id, used as the feature key (`sku_id` + `storefront_id`).
        channel: Channel name, decides which renderer outputs the data.
        country: ISO country code of the buyer.
        currency: original currency of the selling price.
        price_factor: selling price = the SKU's base USD price × this factor (currency conversion included).
        channel_fulfilled_share: share of Order Lines fulfilled by the Channel (FBA, WFS, ...).
        return_lag_mean_days: mean days from delivery until the buyer requests a Return.
        return_logit_shift: added to the Return probability logit, expressing differences between Storefronts.
        cities: major cities; the first city gets the most Orders (geographic skew).
    """

    storefront_id: str
    channel: str
    country: str
    currency: str
    price_factor: float
    channel_fulfilled_share: float
    return_lag_mean_days: float
    return_logit_shift: float
    cities: tuple[str, ...]


STOREFRONTS: dict[str, Storefront] = {
    s.storefront_id: s
    for s in [
        Storefront(
            "amazon_us",
            "amazon",
            "US",
            "USD",
            1.00,
            0.70,
            8.0,
            0.00,
            (
                "New York",
                "Los Angeles",
                "Chicago",
                "Houston",
                "Phoenix",
                "Seattle",
                "Miami",
                "Denver",
            ),
        ),
        Storefront(
            "amazon_uk",
            "amazon",
            "GB",
            "GBP",
            0.80,
            0.70,
            8.0,
            -0.10,
            ("London", "Manchester", "Birmingham", "Leeds", "Glasgow", "Bristol"),
        ),
        Storefront(
            "amazon_de",
            "amazon",
            "DE",
            "EUR",
            0.95,
            0.70,
            9.0,
            0.15,
            ("Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt", "Stuttgart"),
        ),
        Storefront(
            "tiktok_shop_us",
            "tiktok_shop",
            "US",
            "USD",
            0.90,
            0.40,
            6.0,
            0.20,
            ("Los Angeles", "New York", "Houston", "Atlanta", "Dallas", "Chicago"),
        ),
        Storefront(
            "shopify",
            "shopify",
            "US",
            "USD",
            1.05,
            0.00,
            10.0,
            -0.20,
            ("New York", "San Francisco", "Boston", "Austin", "Seattle", "Chicago"),
        ),
        Storefront(
            "walmart_us",
            "walmart",
            "US",
            "USD",
            0.95,
            0.30,
            9.0,
            0.00,
            ("Dallas", "Houston", "Atlanta", "Chicago", "Phoenix", "Orlando"),
        ),
        Storefront(
            "shopee_vn",
            "shopee",
            "VN",
            "VND",
            17_500.0,
            0.00,
            5.0,
            -0.15,
            ("Ho Chi Minh City", "Hanoi", "Da Nang", "Hai Phong", "Can Tho"),
        ),
    ]
}

# Postal code prefix of each city, following that country's real postal system.
# US: first 3 ZIP digits. Germany: first 2 PLZ digits. UK: postcode area of the outward code.
# Vietnam: first 2 digits of the 5-digit national postal code (in use since 2018).
# City names do not repeat across countries, so they work as keys.
POSTAL_PREFIX: dict[str, str] = {
    "New York": "100",
    "Los Angeles": "900",
    "Chicago": "606",
    "Houston": "770",
    "Phoenix": "850",
    "Seattle": "981",
    "Miami": "331",
    "Denver": "802",
    "Atlanta": "303",
    "Dallas": "752",
    "San Francisco": "941",
    "Boston": "021",
    "Austin": "787",
    "Orlando": "328",
    "London": "SW",
    "Manchester": "M",
    "Birmingham": "B",
    "Leeds": "LS",
    "Glasgow": "G",
    "Bristol": "BS",
    "Berlin": "10",
    "Hamburg": "20",
    "Munich": "80",
    "Cologne": "50",
    "Frankfurt": "60",
    "Stuttgart": "70",
    "Ho Chi Minh City": "70",
    "Hanoi": "10",
    "Da Nang": "50",
    "Hai Phong": "04",
    "Can Tho": "94",
}

# Decimal places per currency (VND has no fractional part).
CURRENCY_DECIMALS: dict[str, int] = {"USD": 2, "GBP": 2, "EUR": 2, "VND": 0}


@dataclass(frozen=True)
class Category:
    """A product category.

    Attributes:
        code: prefix of `sku_id`, e.g. `APP` in `APP-00042`.
        sku_share: share of SKUs in the catalog that belong to this category.
        return_rate: target Return rate, used as the baseline of the logistic function.
        price_range_usd: base price range (USD) of SKUs in the category.
    """

    name: str
    code: str
    sku_share: float
    return_rate: float
    price_range_usd: tuple[float, float]


CATEGORIES: tuple[Category, ...] = (
    Category("apparel", "APP", 0.25, 0.22, (15, 80)),
    Category("footwear", "FTW", 0.12, 0.15, (30, 150)),
    Category("accessories", "ACC", 0.12, 0.10, (8, 60)),
    Category("electronics", "ELE", 0.10, 0.08, (20, 300)),
    Category("sports", "SPT", 0.10, 0.08, (10, 120)),
    Category("toys", "TOY", 0.10, 0.06, (8, 70)),
    Category("beauty", "BEA", 0.10, 0.05, (5, 50)),
    Category("home", "HOM", 0.11, 0.04, (10, 200)),
)

# Order share per Storefront when the config does not set one (Storefront skew).
DEFAULT_STOREFRONT_WEIGHTS: dict[str, float] = {
    "amazon_us": 0.45,
    "tiktok_shop_us": 0.15,
    "amazon_uk": 0.12,
    "amazon_de": 0.10,
    "shopify": 0.08,
    "walmart_us": 0.07,
    "shopee_vn": 0.03,
}
