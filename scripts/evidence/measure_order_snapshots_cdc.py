"""Measure three committed Walmart source changes appearing in the CDC topic.

The probe clones one generated order under a unique ID, changes its line status,
then deletes the clone. Thus the source table returns to its original contents.
This measures commit-to-Kafka only; commit-to-Bronze is a separate pipeline proof.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from confluent_kafka import Consumer
from psycopg.types.json import Jsonb

from scripts.setup.setup_order_snapshots_debezium import TOPIC, load_settings


def utc_now() -> str:
    """Return the current UTC timestamp in ISO-8601 form."""
    return datetime.now(timezone.utc).isoformat()


def wait_for_change(consumer: Consumer, probe_id: str, expected_op: str) -> dict:
    """Read until the Debezium event for this probe and operation arrives."""
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        message = consumer.poll(1)
        if message is None:
            continue
        if message.error():
            raise RuntimeError(str(message.error()))
        if message.value() is None:
            continue
        event = json.loads(message.value())
        row = event.get("after") or event.get("before") or {}
        if row.get("purchase_order_id") != probe_id or event.get("op") != expected_op:
            continue
        return {
            "op": expected_op,
            "partition": message.partition(),
            "offset": message.offset(),
            "source_lsn": event.get("source", {}).get("lsn"),
            "event_ts_ms": event.get("ts_ms"),
            "has_before": event.get("before") is not None,
            "has_after": event.get("after") is not None,
        }
    raise TimeoutError(f"No CDC op={expected_op} seen for {probe_id} after 45 seconds")


def main() -> int:
    """Create a source change and measure its arrival in the CDC topic."""
    parser = argparse.ArgumentParser(description="Measure PostgreSQL commit → Kafka CDC")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--output-dir", type=Path, default=Path("out/cdc"))
    args = parser.parse_args()
    settings = load_settings(args.env_file)
    probe_id = "CDC-DEMO-" + uuid.uuid4().hex[:12]
    consumer = Consumer(
        {
            "bootstrap.servers": f"127.0.0.1:{settings.get('KAFKA_PORT', '9094')}",
            "group.id": "platform-cdc-probe-" + uuid.uuid4().hex,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    # Assignment must complete before the first commit; otherwise a latest
    # consumer could start after the first event and silently skip it.
    deadline = time.monotonic() + 20
    while not consumer.assignment() and time.monotonic() < deadline:
        consumer.poll(0.5)
    if not consumer.assignment():
        raise TimeoutError("Consumer has not been assigned a CDC partition")

    events: list[dict] = []
    try:
        with psycopg.connect(
            host="127.0.0.1",
            port=int(settings.get("POSTGRES_PORT", "5432")),
            dbname=settings["POSTGRES_DB"],
            user=settings["POSTGRES_USER"],
            password=settings["POSTGRES_PASSWORD"],
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT raw_payload, export_date FROM walmart.orders LIMIT 1")
                sample = cur.fetchone()
                if sample is None:
                    raise RuntimeError("walmart.orders has no source row to copy")
                payload, export_date = sample
                payload["purchaseOrderId"] = probe_id
                payload["customerOrderId"] = probe_id

                def commit_and_observe(op: str) -> None:
                    """Commit the current mutation, wait for its event, and record latency."""
                    conn.commit()
                    committed_at = utc_now()
                    start = time.monotonic()
                    event = wait_for_change(consumer, probe_id, op)
                    event.update(
                        {
                            "committed_at_utc": committed_at,
                            "commit_to_kafka_ms": round((time.monotonic() - start) * 1000, 1),
                        }
                    )
                    events.append(event)
                    print(f"{op}: {event['commit_to_kafka_ms']} ms, offset={event['offset']}")

                cur.execute(
                    "INSERT INTO walmart.orders "
                    "(purchase_order_id, fetched_at, export_date, raw_payload) "
                    "VALUES (%s, now(), %s, %s)",
                    (probe_id, export_date, Jsonb(payload)),
                )
                commit_and_observe("c")

                cur.execute(
                    "UPDATE walmart.orders SET raw_payload = jsonb_set("
                    "raw_payload, '{orderLines,orderLine,0,orderLineStatuses,orderLineStatus,0,status}', "
                    "'\"Cancelled\"'::jsonb) WHERE purchase_order_id = %s",
                    (probe_id,),
                )
                if cur.rowcount != 1:
                    raise RuntimeError(f"UPDATE touched {cur.rowcount} rows, expected exactly 1")
                commit_and_observe("u")

                cur.execute("DELETE FROM walmart.orders WHERE purchase_order_id = %s", (probe_id,))
                if cur.rowcount != 1:
                    raise RuntimeError(f"DELETE touched {cur.rowcount} rows, expected exactly 1")
                commit_and_observe("d")

                cur.execute(
                    "SELECT count(*) FROM walmart.orders WHERE purchase_order_id = %s",
                    (probe_id,),
                )
                remaining = cur.fetchone()[0]
                if remaining:
                    raise RuntimeError(f"Probe still has {remaining} rows in the source")
    except Exception:
        # If observation fails after an INSERT commit, remove only our probe
        # row so a later DP1 batch run still sees the original source data.
        with psycopg.connect(
            host="127.0.0.1",
            port=int(settings.get("POSTGRES_PORT", "5432")),
            dbname=settings["POSTGRES_DB"],
            user=settings["POSTGRES_USER"],
            password=settings["POSTGRES_PASSWORD"],
        ) as cleanup:
            cleanup.execute("DELETE FROM walmart.orders WHERE purchase_order_id = %s", (probe_id,))
        raise
    finally:
        consumer.close()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / f"probe-{probe_id}.json"
    output.write_text(
        json.dumps(
            {"probe_id": probe_id, "topic": TOPIC, "events": events, "source_rows_remaining": 0},
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"Evidence: {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
