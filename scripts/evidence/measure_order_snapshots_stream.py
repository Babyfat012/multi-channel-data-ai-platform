"""Print evidence of the three Stream problems, compact enough for one screenshot.

This is the counterpart of `python -m generator report` on the Batch side: `report`
re-reads the data written to MinIO and PostgreSQL, while this script replays the Stream
in memory (no Kafka needed) and shows a concrete example of each problem.

The totals of the whole run are in `stream_manifest.json`; this script answers a different
question: "show me a real case". Run:

    uv run python scripts/evidence/measure_order_snapshots_stream.py                      # configs/default.yaml
    uv run python scripts/evidence/measure_order_snapshots_stream.py configs/small.yaml
"""

from __future__ import annotations

import collections
import dataclasses
import json
import sys

from generator.config import load_config
from generator.core.simulate import Simulator
from generator.delivery.stream_events import TOPICS, stream_messages

RULE = "=" * 78


def main(config_path: str) -> None:
    """Scan the Stream once, keeping a representative example of each problem."""
    config = load_config(config_path)
    live = config.drift.tiktok_live
    hours = range(int(live.start_hour_utc), int(live.start_hour_utc + live.duration_hours))

    originals: dict[str, object] = {}
    dup_pair = None  # (original, resend)
    worst_late = None  # (message, latest event_time that arrived before it)
    newest = None
    orders_by_hour: collections.Counter = collections.Counter()

    for msg in stream_messages(config):
        if msg.topic == TOPICS["tiktok_shop"]:
            if msg.is_duplicate:
                if dup_pair is None:
                    dup_pair = (originals[msg.dedup_key], msg)
            else:
                if dup_pair is None:
                    originals[msg.dedup_key] = msg
                if msg.value["data"]["order_status"] == "UNPAID":
                    orders_by_hour[(msg.event_time.date(), msg.event_time.hour)] += 1
        else:
            if newest is not None:
                lag = newest - msg.event_time
                if worst_late is None or lag > worst_late[1] - worst_late[0].event_time:
                    worst_late = (msg, newest)
            newest = msg.event_time if newest is None else max(newest, msg.event_time)

    _print_duplicate(*dup_pair)
    _print_late(*worst_late)
    _print_burst(config, orders_by_hour, hours)


def _print_duplicate(first, second) -> None:
    """Two copies of the same notification: different arrival time, identical in everything else."""
    print(RULE)
    print("1. DUPLICATE: a TikTok Shop resend keeps the same tts_notification_id")
    print(RULE)
    print(f"tts_notification_id : {first.dedup_key}")
    print(f"original arrived at : {first.arrive_at:%Y-%m-%d %H:%M:%S}")
    print(
        f"resend arrived at   : {second.arrive_at:%Y-%m-%d %H:%M:%S}"
        f"   → {(second.arrive_at - first.arrive_at).total_seconds() / 60:.0f} minutes later"
    )
    print(f"identical content   : {first.value == second.value}")
    print(f"payload             : {json.dumps(first.value['data'], separators=(',', ':'))}")


def _print_late(msg, newest) -> None:
    """The most delayed message: arrives after a message with a much later EventTime."""
    print()
    print(RULE)
    print("2. LATE + OUT-OF-ORDER: an Amazon notification arrives after a newer message")
    print(RULE)
    print(f"EventTime   (occurred)   : {msg.event_time:%Y-%m-%d %H:%M:%S}")
    print(
        f"PublishTime (arrived)    : {msg.arrive_at:%Y-%m-%d %H:%M:%S}"
        f"   → {(msg.arrive_at - msg.event_time).total_seconds() / 60:.0f} minutes late"
    )
    print(
        f"newest message arrived   : {newest:%Y-%m-%d %H:%M:%S}"
        f"   → this message is {(newest - msg.event_time).total_seconds() / 3600:.1f} hours older"
    )
    print(f"AmazonOrderId            : {msg.key}")


def _print_burst(config, orders_by_hour, hours) -> None:
    """New Orders per hour on the livestream day, next to the day before it."""
    live = config.drift.tiktok_live
    live_date = config.day_start(live.day).date()
    prev_date = config.day_start(live.day - 1).date()

    print()
    print(RULE)
    print("3. BURST: new TikTok Shop Orders per hour, livestream day vs the day before")
    print(RULE)
    # The label row and the number rows must share the same prefix width, or the columns misalign.
    head = len(f"{'':>11} {live_date}")
    for label, day in (("regular day", prev_date), ("LIVESTREAM", live_date)):
        print(f"{label:>11} {day}:" + "".join(f"{orders_by_hour[(day, h)]:>5}" for h in range(24)))
    print(f"{'UTC hour':>{head}}:" + "".join(f"{h:>5}" for h in range(24)))

    in_window = sum(orders_by_hour[(live_date, h)] for h in hours)
    prev_window = sum(orders_by_hour[(prev_date, h)] for h in hours)
    no_live = dataclasses.replace(config, drift=dataclasses.replace(config.drift, tiktok_live=None))
    base = sum(
        1
        for o in Simulator(no_live).simulate_day(live.day)
        if o.storefront_id == "tiktok_shop_us" and o.created_at.hour in hours
    )

    span = f"{min(hours):02d}-{max(hours) + 1:02d}h"
    print(
        f"\nlive window {span}: {in_window / len(hours):.1f} Orders/hour"
        f"   day before: {prev_window / len(hours):.1f} Orders/hour"
    )
    print(
        f"same day with livestream off: {base / len(hours):.1f} Orders/hour"
        f"   → ratio {in_window / base:.2f}   (expected {Simulator(config).live_multiplier:.2f})"
    )


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "configs/default.yaml")
