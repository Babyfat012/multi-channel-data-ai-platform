"""Configure logical CDC for the simulated Walmart source and register Debezium.

Run after ``docker compose --profile cdc up -d debezium-connect``. The connector
uses ``snapshot.mode=no_data``: the existing large source table is already in
Bronze through DP1, so this path captures only commits made after registration.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

import psycopg
from confluent_kafka.admin import AdminClient, NewTopic

CONNECTOR_NAME = "platform-walmart-orders"
TOPIC = "platform_cdc.walmart.orders"


def load_settings(path: Path) -> dict[str, str]:
    """Read the project's simple KEY=VALUE env file, with file values preferred."""
    values = dict(os.environ)
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            raise ValueError(f"Invalid line in {path}: {key!r}")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def configure_postgres(settings: dict[str, str]) -> None:
    """Enable logical replication and prepare the publication source table."""
    with (
        psycopg.connect(
            host="127.0.0.1",
            port=int(settings.get("POSTGRES_PORT", "5432")),
            dbname=settings["POSTGRES_DB"],
            user=settings["POSTGRES_USER"],
            password=settings["POSTGRES_PASSWORD"],
            autocommit=True,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute("SHOW wal_level")
        wal_level = cur.fetchone()[0]
        if wal_level != "logical":
            raise RuntimeError(f"PostgreSQL wal_level={wal_level}; logical is required")

        # The source has no primary key. FULL supplies before-values for
        # UPDATE/DELETE; it is required for a usable CDC change log.
        cur.execute("ALTER TABLE walmart.orders REPLICA IDENTITY FULL")
        cur.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s",
            ("platform_walmart_orders",),
        )
        if cur.fetchone() is None:
            cur.execute("CREATE PUBLICATION platform_walmart_orders FOR TABLE walmart.orders")
        else:
            cur.execute(
                "SELECT 1 FROM pg_publication_tables "
                "WHERE pubname = %s AND schemaname = %s AND tablename = %s",
                ("platform_walmart_orders", "walmart", "orders"),
            )
            if cur.fetchone() is None:
                cur.execute("ALTER PUBLICATION platform_walmart_orders ADD TABLE walmart.orders")
    print("PostgreSQL: WAL logical, replica identity FULL, publication ready")


def configure_topic(settings: dict[str, str]) -> None:
    """Ensure the configured CDC topic exists with the expected partitioning."""
    admin = AdminClient({"bootstrap.servers": f"127.0.0.1:{settings.get('KAFKA_PORT', '9094')}"})
    future = admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])[TOPIC]
    try:
        future.result(timeout=30)
        print(f"Kafka: created topic {TOPIC} with one partition")
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise
        print(f"Kafka: topic {TOPIC} already exists")


def register_connector(settings: dict[str, str]) -> None:
    """Register or update the Debezium PostgreSQL connector through REST."""
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": settings["POSTGRES_USER"],
        "database.password": settings["POSTGRES_PASSWORD"],
        "database.dbname": settings["POSTGRES_DB"],
        "topic.prefix": "platform_cdc",
        "table.include.list": "walmart.orders",
        "plugin.name": "pgoutput",
        "slot.name": "platform_walmart_orders",
        "publication.name": "platform_walmart_orders",
        "publication.autocreate.mode": "disabled",
        "snapshot.mode": "no_data",
        "tombstones.on.delete": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }
    url = f"http://127.0.0.1:{settings.get('DEBEZIUM_PORT', '8084')}/connectors/{CONNECTOR_NAME}/config"
    request = urllib.request.Request(
        url,
        data=json.dumps(config).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.status not in (200, 201):
                raise RuntimeError(f"Kafka Connect returned HTTP {response.status}")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        detail = detail.replace(settings["POSTGRES_PASSWORD"], "<password hidden>")
        raise RuntimeError(f"Kafka Connect HTTP {exc.code}: {detail}") from exc
    print(f"Debezium: registered connector {CONNECTOR_NAME}; topic {TOPIC}")


def main() -> int:
    """Configure PostgreSQL, Kafka topic, and the Debezium connector."""
    parser = argparse.ArgumentParser(description="Set up CDC for walmart.orders")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args()
    settings = load_settings(args.env_file)
    configure_postgres(settings)
    configure_topic(settings)
    register_connector(settings)
    return 0


if __name__ == "__main__":
    sys.exit(main())
