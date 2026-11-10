"""Drive readiness and offset polling for the CDC v2 harness; does not use the existing compose."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.setup.setup_order_lifecycle_debezium import CONNECTOR_NAME, SLOT, TOPIC
from scripts.setup.setup_order_snapshots_debezium import load_settings
from spark_jobs.lib.order_lifecycle_cdc import compare_events, payload_sha256, write_json


def wait_services(settings, connector=False):
    """Wait for PostgreSQL/Kafka/Connect or an active task and slot, with a hard timeout."""
    import psycopg
    from confluent_kafka.admin import AdminClient

    deadline = time.monotonic() + 120
    last = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(
                host="127.0.0.1",
                port=settings["POSTGRES_PORT"],
                user=settings["POSTGRES_USER"],
                password=settings["POSTGRES_PASSWORD"],
                dbname=settings["POSTGRES_DB"],
                connect_timeout=3,
            ) as conn:
                if (
                    connector
                    and not conn.execute(
                        "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s AND active", (SLOT,)
                    ).fetchone()
                ):
                    raise RuntimeError("Slot not active yet")
            if connector:
                with urllib.request.urlopen(
                    settings["DEBEZIUM_URL"] + f"/connectors/{CONNECTOR_NAME}/status", timeout=3
                ) as response:
                    state = json.load(response)
                if (
                    state["connector"]["state"] != "RUNNING"
                    or not state["tasks"]
                    or any(t["state"] != "RUNNING" for t in state["tasks"])
                ):
                    raise RuntimeError(str(state))
            else:
                AdminClient({"bootstrap.servers": settings["KAFKA_BOOTSTRAP_SERVERS"]}).list_topics(
                    timeout=3
                )
                with urllib.request.urlopen(
                    settings["DEBEZIUM_URL"] + "/connectors", timeout=3
                ) as response:
                    json.load(response)
            return
        except Exception as exc:
            last = exc
            time.sleep(1)
    raise TimeoutError(f"Readiness timed out after 120s: {last}")


def wait_events(settings, oracle_path, receipt):
    """Poll the real offsets until the oracle content is complete, without a fixed sleep."""
    from confluent_kafka import Consumer, TopicPartition

    oracle = json.loads(Path(oracle_path).read_text())
    consumer = Consumer(
        {
            "bootstrap.servers": settings["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": "cdc-v2-harness-poll",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    rows = []
    deadline = time.monotonic() + 120
    try:
        consumer.assign([TopicPartition(TOPIC, 0, 0)])
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            event = json.loads(message.value())
            row = event.get("after") or event.get("before") or {}
            raw = (event.get("after") or {}).get("raw_payload")
            rows.append(
                {
                    "op": event.get("op"),
                    "dataset_version": row.get("dataset_version"),
                    "purchase_order_id": row.get("purchase_order_id"),
                    "payload_after": raw,
                    "record_sha256": payload_sha256(raw),
                    "source_epoch": "poll",
                    "source_lsn": event.get("source", {}).get("lsn"),
                    "source_ts": event.get("source", {}).get("ts_ms"),
                    "topic": TOPIC,
                    "kafka_partition": message.partition(),
                    "kafka_offset": message.offset(),
                }
            )
            checks = compare_events(oracle["events"], rows, "poll")
            if not any(checks.values()):
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(TOPIC, 0), timeout=5, cached=False
                )
                write_json(
                    receipt,
                    {
                        "checks": checks,
                        "start": low,
                        "end": high,
                        "observed_offsets": [r["kafka_offset"] for r in rows],
                    },
                )
                return
        raise TimeoutError(f"CDC did not match the oracle after 120s; offsets={rows}")
    finally:
        consumer.close()


def main():
    """Pick a readiness step or event wait for the shell harness."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["services", "connector", "events"])
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--oracle", type=Path)
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    settings = load_settings(args.env_file)
    if args.mode == "events":
        wait_events(settings, args.oracle, args.receipt)
    else:
        wait_services(settings, args.mode == "connector")


if __name__ == "__main__":
    main()
