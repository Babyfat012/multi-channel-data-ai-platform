"""Local Stream (variant B, Flink SQL looking up PostgreSQL): read Kafka `ml.sku_return_24h`, then write Redis (Feast push
ONLINE) + one new Parquet file into the offline directory. AWS uses the PyFlink job flink_jobs/ml/sku_return_feature.py.

Offline is a DIRECTORY of part files (same layout the offline PyFlink job writes; Feast FileSource reads the whole directory).
Kafka offsets are committed AFTER writing (at-least-once): a rerun may write duplicate offline rows with the same values, PIT join is unchanged.
"""

import json
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from feast import FeatureStore
from feast.data_source import PushMode

SCHEMA = pa.schema(
    [
        ("seller_sku", pa.string()),
        ("event_timestamp", pa.timestamp("us", tz="UTC")),
        ("approved_returns_24h", pa.int64()),
    ]
)


def _fs():
    """Real S3 (IAM) or MinIO (FEAST_S3_ENDPOINT_URL)."""
    return s3fs.S3FileSystem(endpoint_url=os.environ.get("FEAST_S3_ENDPOINT_URL"))


def ensure_offline_file(path):
    """An empty offline directory leaves Feast without a schema; write a 0-row part with the right schema if no part exists."""
    fs, key = _fs(), path.removeprefix("s3://").rstrip("/")
    if not fs.glob(f"{key}/part-*.parquet"):
        pq.write_table(SCHEMA.empty_table(), f"{key}/part-0-0-0.parquet", filesystem=fs)


def write_offline_part(frame, path):
    """One new part file per batch; named like the offline PyFlink job (min/max window_end ms, row count)."""
    ms = frame["event_timestamp"].astype("int64") // 1_000_000
    name = f"part-{ms.min()}-{ms.max()}-{len(frame)}.parquet"
    table = pa.Table.from_pandas(frame, schema=SCHEMA, preserve_index=False)
    pq.write_table(table, f"{path.removeprefix('s3://').rstrip('/')}/{name}", filesystem=_fs())


def to_frame(messages):
    """Flink JSON messages → correctly typed DataFrame; Flink timestamps are UTC without a time zone."""
    rows = [json.loads(m.value()) for m in messages]
    frame = pd.DataFrame(rows, columns=SCHEMA.names)
    frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
    frame["approved_returns_24h"] = frame["approved_returns_24h"].astype("int64")
    return frame


def main():
    """Loop: gather up to 500 messages or 2 seconds, push once, then commit."""
    # Imported here: Airflow calls ensure_offline_file from the Feast venv, which has no confluent_kafka.
    from confluent_kafka import Consumer

    offline = os.environ["FEAST_SKU_RETURN_24H_PATH"]
    ensure_offline_file(offline)
    store = FeatureStore(os.path.dirname(os.path.abspath(__file__)))
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": "feast-pusher",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([os.environ.get("FEAST_PUSH_TOPIC", "ml.sku_return_24h")])
    try:
        while True:
            messages = [m for m in consumer.consume(500, timeout=2.0) if not m.error()]
            if not messages:
                continue
            frame = to_frame(messages)
            store.push("sku_return_24h_push", frame, to=PushMode.ONLINE)
            write_offline_part(frame, offline)
            consumer.commit(asynchronous=False)
            print(f"pushed {len(frame)} rows, last {frame['event_timestamp'].max()}", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
