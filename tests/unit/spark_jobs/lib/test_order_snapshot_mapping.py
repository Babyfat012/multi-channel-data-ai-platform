"""Tests for the Silver layer mapping tables, no Spark cluster needed.

Spark jobs look these dicts up with `create_map`, so `dict.get` here is exactly the job's lookup.
"""

from __future__ import annotations

import pytest

from spark_jobs.lib.order_snapshot_mapping import (
    AMAZON_2026_STATUS,
    AMAZON_FULFILLMENT,
    AMAZON_V0_STATUS,
    COUNTRY_ALIASES,
    WALMART_STATUS,
)


class TestAmazonStatus:
    """Amazon's two schema shapes have two separate status tables."""

    def test_v0_and_2026_share_the_same_meanings(self):
        """Both tables cover exactly the same set of canonical statuses."""
        assert set(AMAZON_V0_STATUS.values()) == set(AMAZON_2026_STATUS.values())

    def test_uppercasing_cannot_merge_the_two_spellings(self):
        """`Canceled` (v0) uppercased becomes `CANCELED`, still different from `CANCELLED` (2026)."""
        assert "Canceled" in AMAZON_V0_STATUS
        assert "CANCELLED" in AMAZON_2026_STATUS
        assert "Canceled".upper() not in AMAZON_2026_STATUS

    @pytest.mark.parametrize(
        "table,raw,expected",
        [
            (AMAZON_V0_STATUS, "Pending", "pending_payment"),
            (AMAZON_V0_STATUS, "Unshipped", "awaiting_shipment"),
            (AMAZON_V0_STATUS, "Shipped", "shipped"),
            (AMAZON_V0_STATUS, "Canceled", "cancelled"),
            (AMAZON_2026_STATUS, "PENDING", "pending_payment"),
            (AMAZON_2026_STATUS, "UNSHIPPED", "awaiting_shipment"),
            (AMAZON_2026_STATUS, "SHIPPED", "shipped"),
            (AMAZON_2026_STATUS, "CANCELLED", "cancelled"),
        ],
    )
    def test_canonical_status(self, table, raw, expected):
        """Each Amazon status maps to the right canonical status."""
        assert table[raw] == expected

    def test_amazon_never_yields_delivered(self):
        """Amazon does not report "delivered": `Shipped` covers shipped/delivered/completed."""
        for table in (AMAZON_V0_STATUS, AMAZON_2026_STATUS):
            assert "delivered" not in table.values()
            assert "completed" not in table.values()


class TestWalmartStatus:
    """Walmart has a real `Delivered`, but it covers two core statuses."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Created", "pending_payment"),
            ("Acknowledged", "awaiting_shipment"),
            ("Shipped", "shipped"),
            ("Delivered", "delivered"),
            ("Cancelled", "cancelled"),
        ],
    )
    def test_canonical_status(self, raw, expected):
        """Each Walmart status maps to the right canonical status."""
        assert WALMART_STATUS[raw] == expected

    def test_delivered_picks_the_earlier_status(self):
        """`Delivered` covers delivered + completed; pick `delivered` because it is always true."""
        assert "completed" not in WALMART_STATUS.values()


def test_fulfillment_two_shapes_map_to_one_value_set():
    """AFN/AMAZON means the Channel ships, MFN/MERCHANT means the Seller ships."""
    assert AMAZON_FULFILLMENT == {
        "AFN": "channel",
        "AMAZON": "channel",
        "MFN": "seller",
        "MERCHANT": "seller",
    }


def test_country_codes():
    """`USA` maps to `US`; other codes are not in the table, so the job keeps them."""
    assert COUNTRY_ALIASES == {"USA": "US"}
