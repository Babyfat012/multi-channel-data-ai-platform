"""Feast definitions for SKUs: two feature views, one Batch and one Stream.

- `sku_return_daily`: 90-day return rate snapshotted daily; Parquet written by
  `spark_jobs/gold/export_sku_daily_features.py`, Airflow materializes it to Redis.
- `sku_return_24h`: number of approved Returns in the last 24h; computed by Flink, `push_stream_features.py`
  pushes it to both Redis and offline Parquet (so training uses a PIT join like the batch feature).
"""

import os
from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Float64, Int64, String

sku = Entity(
    name="sku", join_keys=["seller_sku"], value_type=ValueType.STRING, description="Seller SKU"
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
    # 0 = never expires: slow-selling SKUs keep their last snapshot. For the first materialize only,
    # Feast scans from 1 year before wall-clock time, so the simulated data must fall within that range.
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

# Push overwrites the whole file every time (read + rewrite), so it must be ONE file that exists beforehand:
# push_stream_features.py creates an empty file on startup.
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
