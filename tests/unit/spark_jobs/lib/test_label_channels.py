"""Five-Channel label table: a Channel that joins to 0 rows must be blocked, not slip through silently."""

from spark_jobs.lib.label_channels import CHANNELS, missing_channels


def test_every_channel_with_labels_passes():
    """All five Channels with label rows means no Channel is missing."""
    assert missing_channels({channel: 1 for channel in CHANNELS}) == []


def test_empty_or_absent_channel_is_reported():
    """A wrong join key ⇒ a Channel with 0 rows or absent from the groupBy entirely; both are reported."""
    counts = {channel: 5 for channel in CHANNELS} | {"shopee": 0}
    del counts["amazon"]
    assert missing_channels(counts) == ["amazon", "shopee"]
    assert sorted(CHANNELS) == ["amazon", "shopee", "shopify", "tiktok_shop", "walmart"]
