"""Luồng local (bản B, Flink SQL tra PostgreSQL): đọc Kafka `ml.sku_return_24h` rồi ghi Redis (Feast push
ONLINE) + một file Parquet mới vào thư mục offline. AWS dùng job PyFlink flink_jobs/ml/sku_return_feature.py.

Offline là THƯ MỤC nhiều file part (cùng bố cục job PyFlink offline ghi; Feast FileSource đọc cả thư mục).
Commit offset Kafka SAU khi ghi (at-least-once): chạy lại có thể ghi trùng dòng offline cùng giá trị, PIT join không đổi.
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
    """S3 thật (IAM) hoặc MinIO (FEAST_S3_ENDPOINT_URL)."""
    return s3fs.S3FileSystem(endpoint_url=os.environ.get("FEAST_S3_ENDPOINT_URL"))


def ensure_offline_file(path):
    """Thư mục offline rỗng thì Feast không biết schema; ghi một part 0 dòng đúng schema nếu chưa có part nào."""
    fs, key = _fs(), path.removeprefix("s3://").rstrip("/")
    if not fs.glob(f"{key}/part-*.parquet"):
        pq.write_table(SCHEMA.empty_table(), f"{key}/part-0-0-0.parquet", filesystem=fs)


def write_offline_part(frame, path):
    """Một file part mới mỗi lô; tên giống job PyFlink offline (min/max window_end ms, số dòng)."""
    ms = frame["event_timestamp"].astype("int64") // 1_000_000
    name = f"part-{ms.min()}-{ms.max()}-{len(frame)}.parquet"
    table = pa.Table.from_pandas(frame, schema=SCHEMA, preserve_index=False)
    pq.write_table(table, f"{path.removeprefix('s3://').rstrip('/')}/{name}", filesystem=_fs())


def to_frame(messages):
    """Tin JSON của Flink → DataFrame đúng kiểu; timestamp Flink là UTC không múi giờ."""
    rows = [json.loads(m.value()) for m in messages]
    frame = pd.DataFrame(rows, columns=SCHEMA.names)
    frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
    frame["approved_returns_24h"] = frame["approved_returns_24h"].astype("int64")
    return frame


def main():
    """Vòng lặp: gom tối đa 500 tin hoặc 2 giây, push một lần, rồi commit."""
    # Import ở đây: Airflow gọi ensure_offline_file từ venv Feast không có confluent_kafka.
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
