"""Phát lại Return của pipeline A vào Kafka để Flink tính feature 24h (stream A không có sự kiện Return).

Đầu vào: JSON từng dòng (Trino `--output-format JSON` của sql/ml/return_events_replay.sql) qua stdin.
Chạy trong image feast-pusher (có confluent_kafka):
docker compose ... run --rm -T feast-pusher python replay_return_events.py < return_events.jsonl
"""

import hashlib
import json
import os
import sys
from datetime import UTC, datetime

TOPIC = "order_snapshots.return_events"


def to_event(row):
    """Dòng Trino → sự kiện "approved"; thời gian chuẩn ISO UTC mili giây (Flink cắt 23 ký tự đầu)."""
    at = datetime.fromisoformat(row["label_available_at"].replace(" UTC", "+00:00"))
    at = (at if at.tzinfo else at.replace(tzinfo=UTC)).astimezone(UTC)
    effective_at = at.strftime("%Y-%m-%dT%H:%M:%S.") + f"{at.microsecond // 1000:03d}Z"
    # Khoá bỏ trùng = Order Line: phát lại hai lần cùng line vẫn chỉ đếm một Return.
    sha = hashlib.sha256(row["order_line_key"].encode()).hexdigest()
    return {
        "record_sha256": sha,
        "effective_at": effective_at,
        "event_type": "approved",
        "quantity": 1,
        "seller_sku": row["seller_sku"],
    }


def event_ms(event):
    """Timestamp bản ghi Kafka = event time (mili giây): job PyFlink sinh watermark từ đây, theo từng partition."""
    at = datetime.strptime(event["effective_at"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    return int(at.timestamp() * 1000)


def main():
    """Đọc stdin, gửi theo thứ tự, flush rồi in số tin."""
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
