"""Replay pipeline A Returns into Kafka so Flink computes the 24h feature (stream A has no Return events).

Input: JSON lines (Trino `--output-format JSON` of sql/ml/return_events_replay.sql) on stdin.
Runs in the feast-pusher image (has confluent_kafka):
docker compose ... run --rm -T feast-pusher python replay_return_events.py < return_events.jsonl
"""

import hashlib
import json
import os
import sys
from datetime import UTC, datetime

TOPIC = "order_snapshots.return_events"


def to_event(row):
    """Trino row → "approved" event; time in ISO UTC with milliseconds (Flink takes the first 23 characters)."""
    at = datetime.fromisoformat(row["label_available_at"].replace(" UTC", "+00:00"))
    at = (at if at.tzinfo else at.replace(tzinfo=UTC)).astimezone(UTC)
    effective_at = at.strftime("%Y-%m-%dT%H:%M:%S.") + f"{at.microsecond // 1000:03d}Z"
    # Dedup key = Order Line: replaying the same line twice still counts one Return.
    sha = hashlib.sha256(row["order_line_key"].encode()).hexdigest()
    return {
        "record_sha256": sha,
        "effective_at": effective_at,
        "event_type": "approved",
        "quantity": 1,
        "seller_sku": row["seller_sku"],
    }


def event_ms(event):
    """Kafka record timestamp = event time (milliseconds): the PyFlink job derives watermarks from it, per partition."""
    at = datetime.strptime(event["effective_at"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    return int(at.timestamp() * 1000)


def main():
    """Read stdin, send in order, flush, then print the message count."""
    from confluent_kafka import Producer

    producer = Producer(
        {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")}
    )
    sent = 0
    for line in sys.stdin:
        if line.strip():
            event = to_event(json.loads(line))
            producer.produce(
                TOPIC,
                key=event["seller_sku"],
                value=json.dumps(event),
                timestamp=event_ms(event),
            )
            producer.poll(0)
            sent += 1
    producer.flush(60)
    print(f"sent {sent} return events to {TOPIC}", flush=True)


if __name__ == "__main__":
    main()
