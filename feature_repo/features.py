"""Định nghĩa Feast cho SKU: hai feature view, một batch và một stream.

- `sku_return_daily`: tỷ lệ trả 90 ngày chụp mỗi ngày; Parquet do
  `spark_jobs/gold/export_sku_daily_features.py` ghi, Airflow materialize sang Redis.
- `sku_return_24h`: số Return approved 24h gần nhất; Flink tính, `push_stream_features.py`
  push vào cả Redis lẫn Parquet offline (để train dùng PIT join như feature batch).
"""

import os
from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Float64, Int64, String

sku = Entity(
    name="sku", join_keys=["seller_sku"], value_type=ValueType.STRING, description="SKU của Seller"
)

sku_daily_source = FileSource(
    name="sku_daily_source",
    path=os.environ.get("FEAST_SKU_DAILY_PATH", "s3://platform/feature-store/sku_daily"),
    timestamp_field="event_timestamp",
    s3_endpoint_override=os.environ.get("FEAST_S3_ENDPOINT_URL") or None,
)

sku_return_daily = FeatureView(
    name="sku_return_daily",
    entities=[sku],
    # 0 = không hết hạn: SKU ít bán vẫn giữ ảnh chụp cuối cùng. Riêng lần materialize đầu tiên,
    # Feast chỉ quét từ 1 năm trước giờ thật, nên dữ liệu giả lập phải nằm trong khoảng đó.
    ttl=timedelta(0),
    schema=[
        Field(name="labeled_count_90d", dtype=Int64),
        Field(name="positive_count_90d", dtype=Int64),
        Field(name="return_or_refund_rate_90d", dtype=Float64),
        Field(name="dataset_version", dtype=String),
        Field(name="generation_id", dtype=String),
    ],
    source=sku_daily_source,
)

# Push ghi đè cả file mỗi lần (đọc + ghi lại), nên phải là MỘT file và có sẵn trước:
# push_stream_features.py tạo file rỗng khi khởi động.
sku_return_24h_push = PushSource(
    name="sku_return_24h_push",
    batch_source=FileSource(
        name="sku_return_24h_source",
        path=os.environ.get(
            "FEAST_SKU_RETURN_24H_PATH", "s3://platform/feature-store/sku_return_24h/"
        ),
        timestamp_field="event_timestamp",
        s3_endpoint_override=os.environ.get("FEAST_S3_ENDPOINT_URL") or None,
    ),
)

sku_return_24h = FeatureView(
    name="sku_return_24h",
    entities=[sku],
    ttl=timedelta(days=1),
    schema=[Field(name="approved_returns_24h", dtype=Int64)],
    source=sku_return_24h_push,
)
