"""The five Channels and the empty/mismatched Channel guards (label table, warehouse), kept out of pyspark for testing."""

from __future__ import annotations

# Walmart joins through its own bridge (needs customer_order_id); the other four Channels join via the key ledger
# ground-truth/order_line_keys/<channel>/ on channel_order_id + channel_line_id.
KEY_CHANNELS = ("amazon", "tiktok_shop", "shopify", "shopee")
CHANNELS = (*KEY_CHANNELS, "walmart")


def missing_channels(counts: dict[str, int]) -> list[str]:
    """Channels with no label rows: a wrong join key yields 0 rows without raising an error."""
    return [channel for channel in CHANNELS if counts.get(channel, 0) <= 0]


def count_mismatches(gold: dict[str, int], warehouse: dict[str, int]) -> list[str]:
    """Channels empty in Gold or with a warehouse row count different from Gold: a short JDBC write reports nothing."""
    return [c for c in CHANNELS if gold.get(c, 0) <= 0 or warehouse.get(c, 0) != gold[c]]
