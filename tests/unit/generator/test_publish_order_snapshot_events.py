"""Tests for the Stream part: arrival order, duplicates, delays, bursts, emit pace."""

from __future__ import annotations

from datetime import timedelta

import pytest

from generator.config import parse_config
from generator.delivery.stream_events import TOPICS, stream_messages, wall_offset
from generator.publish_order_snapshot_events import FileSink, run_stream
from generator.storage import OutputStore


@pytest.fixture
def stream_config(tmp_path):
    """12 history days, 6 Stream days, livestream on day 15, duplicates and delays amplified to be measurable at small scale."""
    return parse_config(
        {
            "seed": 7,
            "scale": {"n_skus": 20, "days_history": 12, "days_stream": 6, "orders_per_day": 400},
            "offline_problems": {"amazon_orders_api_switch_day": 6},
            "streaming_problems": {
                "pacing": {
                    "default_seconds_per_sim_day": 60,
                    "segments": [{"from_day": 15, "to_day": 15, "seconds_per_sim_day": 600}],
                },
                "duplicate_rate": {"tiktok_shop_us": 0.1},
                "amazon_late": {"median_minutes": 5, "share_over_1h": 0.2},
            },
            "drift": {
                "tiktok_live": {
                    "day": 15,
                    "start_hour_utc": 12,
                    "duration_hours": 6,
                    "burst_multiplier": [6, 6],
                }
            },
            "storage": {"output_root": str(tmp_path / "out")},
        }
    )


def test_messages_arrive_in_order_and_inside_stream(stream_config):
    """Prove messages arrive in time order, events fall within cutoff→stream_end, never arrive before they happen, and cover every topic."""
    msgs = list(stream_messages(stream_config))
    assert [m.arrive_at for m in msgs] == sorted(m.arrive_at for m in msgs)
    assert all(stream_config.cutoff <= m.event_time < stream_config.stream_end for m in msgs)
    assert all(m.arrive_at >= m.event_time for m in msgs)
    assert {m.topic for m in msgs} == set(TOPICS.values())


def test_tiktok_duplicates_keep_notification_id(stream_config):
    """Prove TikTok duplicates keep the same payload and arrive after the original, and the duplicate rate matches the config."""
    tiktok = [m for m in stream_messages(stream_config) if m.topic == TOPICS["tiktok_shop"]]
    dups = [m for m in tiktok if m.is_duplicate]
    assert dups
    originals = {m.dedup_key: m for m in tiktok if not m.is_duplicate}
    for d in dups:
        assert d.value == originals[d.dedup_key].value
        assert d.arrive_at > originals[d.dedup_key].arrive_at
    rate = 1 - len({m.dedup_key for m in tiktok}) / len(tiktok)
    assert rate == pytest.approx(0.1, abs=0.03)


def test_amazon_late_share_and_out_of_order(stream_config):
    """Prove Amazon has no duplicates, the share delayed over 1 hour matches the config, some messages are out of order, and messages carry `PublishTime`."""
    amazon = [m for m in stream_messages(stream_config) if m.topic == TOPICS["amazon"]]
    assert not any(m.is_duplicate for m in amazon)
    late = [m for m in amazon if m.arrive_at - m.event_time > timedelta(hours=1)]
    assert len(late) / len(amazon) == pytest.approx(0.2, abs=0.04)
    # Late messages arrive after messages with a later event_time.
    assert any(a.event_time > b.event_time for a, b in zip(amazon, amazon[1:]))
    assert amazon[0].value["NotificationMetadata"]["PublishTime"]


def test_burst_matches_live_multiplier(stream_config, tmp_path):
    """Prove the livestream raises new TikTok Orders by the configured multiplier, and the sink writes the correct envelope shape."""
    manifest = run_stream(
        stream_config, FileSink(OutputStore(stream_config.storage.output_root)), speed=0
    )
    burst = manifest["burst"]
    assert burst["multiplier_truth"] == 6
    assert 4 < burst["measured_ratio"] < 8
    rows = list(
        OutputStore(stream_config.storage.output_root).read_jsonl(
            f"stream/{TOPICS['tiktok_shop']}/part-00000.jsonl"
        )
    )
    assert rows and rows[0]["value"]["type"] == 1


def test_same_seed_same_stream(stream_config):
    """Prove the same seed yields the exact same message stream (keys and arrival times)."""
    first = [(m.dedup_key, m.arrive_at) for m in stream_messages(stream_config)]
    assert first == [(m.dedup_key, m.arrive_at) for m in stream_messages(stream_config)]


def test_pacing_slows_down_live_day(stream_config):
    """Prove the emit pace: the start is 0, normal days take 60 seconds/day, the livestream day is 5 times slower."""
    start = stream_config.cutoff
    assert wall_offset(stream_config, start) == 0
    assert wall_offset(stream_config, stream_config.day_start(14)) == 60  # day 13 is normal
    live_start = stream_config.day_start(15)
    assert (
        wall_offset(stream_config, live_start + timedelta(hours=12))
        - wall_offset(stream_config, live_start)
        == 300
    )


def test_live_day_must_be_in_stream():
    """Prove a config placing the livestream day outside the Stream is rejected."""
    with pytest.raises(ValueError, match="tiktok_live.day"):
        parse_config(
            {
                "scale": {"days_history": 12, "days_stream": 6},
                "offline_problems": {"amazon_orders_api_switch_day": 6},
                "drift": {"tiktok_live": {"day": 5}},
            }
        )
