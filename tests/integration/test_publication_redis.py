"""Check publication against a dedicated local Redis 7.4; opt in with PLATFORM_TEST_REDIS_URL."""

import os
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import pandas as pd
import pytest

from feature_repo.publication import (
    BATCH_FIELDS,
    FeatureDataUnavailable,
    PublicationBusy,
    PublishedFeatureStore,
    RedisLeases,
    Release,
    ReleaseCache,
    make_store,
)
from feature_repo.publication import (
    redis_client as feast_redis_client,
)
from feature_repo.publish_batch import latest_expected, reconcile


@pytest.fixture
def redis_client():
    """Connect only to the specified test endpoint; never use the platform's Redis variables."""
    url = os.environ.get("PLATFORM_TEST_REDIS_URL")
    if not url:
        pytest.skip("set PLATFORM_TEST_REDIS_URL to a disposable Redis 7.4")
    assert urlsplit(url).hostname in ("127.0.0.1", "localhost"), "test Redis must be local"
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(url)
    assert client.info("server")["redis_version"].startswith("7.4.")
    yield client
    client.close()


def test_lua_lease_prevents_reuse_and_fences_cached_old_release(redis_client):
    """Exercise real Lua: an active reader blocks staging, an expired reader gets no data."""
    prefix = "test:publication:" + uuid.uuid4().hex
    leases = RedisLeases(redis_client, prefix=prefix)
    a = Release("a", 1, "registry-a", "offline-a", "2026-06-30", frozenset({"sku"}))
    try:
        with leases.writer() as owner:
            leases.prepare(1, "a", owner)
            leases.ready(1, "a", owner)
        token = leases.acquire(a)
        assert token is not None
        with leases.writer() as owner:
            with pytest.raises(PublicationBusy):
                leases.prepare(1, "c", owner)
            leases.finish(a, token)
            leases.prepare(1, "c", owner)
            assert leases.acquire(a) is None
            leases.ready(1, "c", owner)
        c = Release("c", 1, "registry-c", "offline-c", "2026-07-01", frozenset({"sku"}))
        token = leases.acquire(c)
        assert token is not None
        redis_client.zadd(leases.keys(1)[1], {token: 0})
        with pytest.raises(FeatureDataUnavailable, match="expired"):
            leases.finish(c, token)
    finally:
        keys = list(redis_client.scan_iter(prefix + "*"))
        if keys:
            redis_client.delete(*keys)


@pytest.mark.parametrize(("end", "count_a"), [("2026-06-01", 10), ("2026-06-30", 20)])
def test_feast_materialize_and_reconcile_in_isolated_database(redis_client, tmp_path, end, count_a):
    """Real Feast 0.66 writes DB1 from a local Parquet source with its own registry; DB0 gets no features."""
    from feast import Entity, FeatureView, Field, FileSource, ValueType
    from feast.types import Float64, Int64, String

    port = redis_client.connection_pool.connection_kwargs["port"]
    connection = f"127.0.0.1:{port},db=1"
    source = tmp_path / "features.parquet"
    rows = pd.DataFrame(
        {
            "seller_sku": ["a", "a", "b"],
            "event_timestamp": pd.to_datetime(["2026-06-01", "2026-06-02", "2026-06-01"], utc=True),
            "labeled_count_90d": [10, 20, 0],
            "positive_count_90d": [1, 3, 0],
            "return_or_refund_rate_90d": [0.1, 0.15, None],
            "dataset_version": ["v1"] * 3,
            "generation_id": ["g1"] * 3,
        }
    )
    rows.to_parquet(source)
    (tmp_path / "feature_store.yaml").write_text(
        f"project: publication_test\nprovider: local\nregistry: {tmp_path / 'registry.db'}\n"
        "offline_store:\n  type: file\nonline_store:\n  type: redis\n"
        f"  connection_string: {connection}\nentity_key_serialization_version: 3\n"
    )
    store = make_store(tmp_path, connection)
    target = feast_redis_client(store, 1)
    try:
        target.flushdb()  # Only the opted-in local test Redis DB from the fixture.
    finally:
        target.close()
    sku = Entity(name="sku", join_keys=["seller_sku"], value_type=ValueType.STRING)
    types = [Int64, Int64, Float64, String, String]
    view = FeatureView(
        name="sku_return_daily",
        entities=[sku],
        ttl=timedelta(0),
        schema=[
            Field(name=name, dtype=dtype) for name, dtype in zip(BATCH_FIELDS, types, strict=True)
        ],
        source=FileSource(name="daily", path=str(source), timestamp_field="event_timestamp"),
    )
    store.apply([sku, view])
    store.materialize(
        datetime(2000, 1, 1, tzinfo=timezone.utc),
        datetime.fromisoformat(end).replace(tzinfo=timezone.utc),
        feature_views=["sku_return_daily"],
    )
    expected = latest_expected(rows, end)
    reconcile(store, expected)
    assert store.get_feature_view("sku_return_daily").most_recent_end_time is not None
    target = feast_redis_client(store, 1)
    try:
        assert target.dbsize() == 2
    finally:
        target.close()
    prefix = "test:published-reader:" + uuid.uuid4().hex
    leases = RedisLeases(redis_client, prefix=prefix)
    item = Release("a", 1, str(tmp_path / "registry.db"), str(source), end, frozenset({"a", "b"}))
    try:
        with leases.writer() as owner:
            leases.prepare(1, "a", owner)
            leases.ready(1, "a", owner)
        reader = PublishedFeatureStore(None, ReleaseCache(lambda: item), leases, lambda _: store)
        result = reader.get_online_features(
            features=["sku_return_daily:labeled_count_90d"],
            entity_rows=[{"seller_sku": "b"}, {"seller_sku": "a"}],
        )
        assert result.to_dict()["labeled_count_90d"] == [0, count_a]
        assert result.release.release_id == "a"
    finally:
        keys = list(redis_client.scan_iter(prefix + "*"))
        if keys:
            redis_client.delete(*keys)


def test_s3_sdk_supports_publication_conditions():
    """The pinned SDK must accept the ETag condition the publisher uses, without calling real S3."""
    import boto3

    client = boto3.client(
        "s3",
        region_name="ap-southeast-2",
        endpoint_url="http://127.0.0.1:19999",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    fields = client.meta.service_model.operation_model("PutObject").input_shape.members
    assert "IfMatch" in fields and "IfNoneMatch" in fields
