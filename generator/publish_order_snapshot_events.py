"""Run the Stream part: emit messages after `cutoff` to Kafka or to files.

Two sinks:
- `kafka`: send to Kafka; the message key is the native order id, the value is the native JSON.
- `file`: write `stream/<topic>/part-00000.jsonl` under `storage.output_root`,
  each row with `arrive_at`, `key` and `value`. Used for tests and for measuring without Kafka.

`speed` divides the wait time: 1 is the configured pace (about 40 minutes for
30 days), 60 is 60 times faster, 0 is no waiting.

After emitting, write `stream_manifest.json` with the real measurements: duplicate rate,
late rate, number of messages out of order by more than 1 hour, burst multiplier. This is the answer key
to compare with what Flink measures.
"""

from __future__ import annotations

import json
import statistics
import time
from collections import Counter, defaultdict
from datetime import timedelta
from typing import Any, Protocol

from generator.config import GeneratorConfig
from generator.core.simulate import Simulator
from generator.delivery.stream_events import TOPICS, StreamMessage, stream_messages, wall_offset
from generator.storage import OutputStore


class Sink(Protocol):
    """Message sink."""

    def send(self, msg: StreamMessage) -> None:
        """Accept a message and forward it to the real store (Kafka or file)."""
        ...

    def close(self) -> None:
        """Flush pending messages and release the sink's resources."""
        ...


class FileSink:
    """Write messages to JSONL files per topic (kept in RAM, written once on close)."""

    def __init__(self, store: OutputStore) -> None:
        """Remember the store and open an empty buffer per topic.

        Collect in RAM and write once on `close` to avoid reopening the file per row.
        """
        self.store = store
        self.rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    def send(self, msg: StreamMessage) -> None:
        """Queue the message into its topic's buffer without touching disk."""
        self.rows[msg.topic].append(
            {"arrive_at": msg.arrive_at.isoformat(), "key": msg.key, "value": msg.value}
        )

    def close(self) -> None:
        """Write each topic as one JSONL file `stream/<topic>/part-00000.jsonl`."""
        for topic, rows in self.rows.items():
            self.store.write_jsonl(f"stream/{topic}/part-00000.jsonl", rows)


class KafkaSink:
    """Send messages to Kafka via `confluent-kafka`."""

    def __init__(self, bootstrap_servers: str) -> None:
        """Create a Producer for `bootstrap_servers`.

        `linger.ms=50` batches messages for 50ms, reducing network sends.
        """
        from confluent_kafka import Producer

        self.producer = Producer({"bootstrap.servers": bootstrap_servers, "linger.ms": 50})

    def send(self, msg: StreamMessage) -> None:
        """Produce the message keyed by the native order id so Kafka keeps per-key order.

        `poll(0)` serves delivery callbacks without blocking the emit loop.
        """
        self.producer.produce(msg.topic, key=msg.key.encode(), value=json.dumps(msg.value).encode())
        self.producer.poll(0)

    def close(self) -> None:
        """Wait for the Producer to flush the remaining batched messages before finishing."""
        self.producer.flush()


class StreamStats:
    """Count the Stream measurements while emitting."""

    def __init__(self, config: GeneratorConfig) -> None:
        """Initialize empty counters; all measurements accumulate while emitting and are read once in `summary`."""
        self.config = config
        self.rows: Counter = Counter()
        self.duplicates: Counter = Counter()
        self.keys: dict[str, set[str]] = defaultdict(set)
        self.delays_min: dict[str, list[float]] = defaultdict(list)
        self.out_of_order: Counter = Counter()
        self.max_event: dict[str, Any] = {}
        self.new_tiktok_orders_by_hour: Counter = Counter()

    def add(self, msg: StreamMessage) -> None:
        """Update the measurements for one message: rows, unique keys, delay, out of order by more than 1 hour, and new TikTok Orders per hour."""
        t = msg.topic
        self.rows[t] += 1
        self.duplicates[t] += msg.is_duplicate
        self.keys[t].add(msg.dedup_key)
        if not msg.is_duplicate:
            self.delays_min[t].append((msg.arrive_at - msg.event_time).total_seconds() / 60)
        # Significantly out of order: arrives after a message whose event_time is more than 1 hour later.
        if t in self.max_event and msg.event_time < self.max_event[t] - timedelta(hours=1):
            self.out_of_order[t] += 1
        self.max_event[t] = max(self.max_event.get(t, msg.event_time), msg.event_time)
        if (
            t == TOPICS["tiktok_shop"]
            and not msg.is_duplicate
            and msg.value["data"]["order_status"] == "UNPAID"
        ):
            self.new_tiktok_orders_by_hour[
                msg.event_time.replace(minute=0, second=0, microsecond=0)
            ] += 1

    def summary(self, live_multiplier: float) -> dict[str, Any]:
        """Combine per-topic measurements into a dict for the manifest.

        Includes duplicate rate, median delay, share delayed over 1 hour, out-of-order count,
        and the livestream burst multiplier.
        """
        topics = {}
        for t, rows in self.rows.items():
            delays = self.delays_min[t]
            topics[t] = {
                "rows": rows,
                "unique_keys": len(self.keys[t]),
                "duplicate_rate": round(1 - len(self.keys[t]) / rows, 6),
                "injected_duplicates": self.duplicates[t],
                "median_delay_minutes": round(statistics.median(delays), 2) if delays else None,
                "share_late_over_1h": round(sum(d > 60 for d in delays) / len(delays), 6)
                if delays
                else None,
                "out_of_order_over_1h": self.out_of_order[t],
            }
        return {"topics": topics, "burst": self._burst(live_multiplier)}

    def _burst(self, live_multiplier: float) -> dict[str, Any] | None:
        """Compare new TikTok Orders per hour during the livestream with the same hours on other days."""
        live = self.config.drift.tiktok_live
        if live is None:
            return None
        first = self.config.scale.days_history + 1
        last = self.config.scale.days_history + self.config.scale.days_stream
        hours = range(int(live.start_hour_utc), int(live.start_hour_utc + live.duration_hours))

        def per_hour(day: int) -> float:
            """Mean new TikTok Orders per hour in the livestream hours of one day."""
            start = self.config.day_start(day)
            return sum(
                self.new_tiktok_orders_by_hour[start + timedelta(hours=h)] for h in hours
            ) / len(hours)

        normal_days = [d for d in range(first, last + 1) if d != live.day]
        normal = statistics.mean(per_hour(d) for d in normal_days) if normal_days else 0.0
        during = per_hour(live.day)
        return {
            "live_day": live.day,
            "multiplier_truth": round(live_multiplier, 3),
            "new_orders_per_hour_live": round(during, 1),
            "new_orders_per_hour_normal": round(normal, 1),
            "measured_ratio": round(during / normal, 2) if normal else None,
        }


def run_stream(config: GeneratorConfig, sink: Sink, speed: float = 1.0) -> dict[str, Any]:
    """Emit the whole Stream to `sink` and return a manifest with the measurements."""
    stats = StreamStats(config)
    started = time.monotonic()
    try:
        for msg in stream_messages(config):
            if speed > 0:
                wait = wall_offset(config, msg.arrive_at) / speed - (time.monotonic() - started)
                if wait > 0:
                    time.sleep(wait)
            sink.send(msg)
            stats.add(msg)
    finally:
        sink.close()

    manifest = {
        "cutoff": config.cutoff.isoformat(),
        "stream_end": config.stream_end.isoformat(),
        "speed": speed,
        **stats.summary(Simulator(config).live_multiplier),
    }
    OutputStore(config.storage.output_root).write_json("stream_manifest.json", manifest)
    return manifest
