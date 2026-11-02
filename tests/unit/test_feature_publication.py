"""Công bố A/B không ghi đè active; cache/pin/fence và đối soát phát hiện dữ liệu sai."""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from feature_repo.publication import (
    BATCH_FIELDS,
    FeatureDataUnavailable,
    FeatureResult,
    PublicationBusy,
    PublishedFeatureStore,
    Release,
    ReleaseCache,
    redis_database,
)
from feature_repo.publish_batch import latest_expected, publish_candidate, reconcile


def release(name="a", database=1):
    """Một release đã công bố với registry độc lập."""
    return Release(
        name,
        database,
        f"s3://test/{name}/registry.db",
        f"s3://test/{name}/offline",
        "2026-06-30",
        frozenset({"sku"}),
    )


def document(name="a", database=1):
    """Manifest hợp lệ cho catalog giả."""
    return {
        "release_id": name,
        "database": database,
        "registry_uri": f"s3://test/{name}/registry.db",
        "offline_uri": f"s3://test/{name}/offline",
        "cutoff": "2026-06-30",
        "validation": "passed",
        "expected_skus": ["sku"],
    }


class Leases:
    """Coordinator giả để kiểm reader/writer protocol; Lua được thử riêng với Redis thật."""

    def __init__(self):
        """Ban đầu A/B ready, chưa có reader."""
        self.retired, self.active, self.prepared = set(), set(), []
        self.expired = False

    def acquire(self, item):
        """Generation thu hồi không nhận reader mới."""
        if item.release_id in self.retired:
            return None
        self.active.add(item.release_id)
        return item.release_id

    def finish(self, item, token):
        """Result đã hết lease phải bị bỏ, không trả dữ liệu có thể bị dùng lại."""
        self.active.discard(token)
        if self.expired:
            raise FeatureDataUnavailable("expired")

    def cancel(self, item, token):
        """Dọn lượt đọc lỗi."""
        self.active.discard(token)

    @contextmanager
    def writer(self):
        """Một writer trong test đơn tiến trình."""
        yield "owner"

    def prepare(self, database, name, owner):
        """Không nhận staging khi reader còn giữ slot."""
        if self.active:
            raise PublicationBusy("still reading")
        self.prepared.append((database, name))

    def ready(self, database, name, owner):
        """Chỉ được ready đúng candidate đã chuẩn bị."""
        assert (database, name) in self.prepared


def test_cache_returns_immutable_pin_and_refreshes_after_ttl():
    """Cache chuyển B không làm request đã chọn A biến thành B."""
    clock = [0]
    load = Mock(side_effect=[release(), release("b", 2)])
    cache = ReleaseCache(load, ttl=5, clock=lambda: clock[0])
    pinned = cache.current()
    clock[0] = 4
    assert cache.current() is pinned
    clock[0] = 5
    assert cache.current().release_id == "b"
    assert pinned.release_id == "a"
    assert load.call_count == 2


def test_expired_cache_does_not_serve_stale_forever_on_s3_error():
    """Sau TTL, lỗi nguồn không được âm thầm dùng con trỏ cũ vô hạn."""
    clock = [0]
    cache = ReleaseCache(Mock(side_effect=[release(), OSError("s3 down")]), clock=lambda: clock[0])
    cache.current()
    clock[0] = 6
    with pytest.raises(OSError, match="s3 down"):
        cache.current()


def test_database_selection_preserves_auth_and_replaces_old_db():
    """Không bỏ credential/tùy chọn khác và không để hai tham số db xung đột."""
    assert (
        redis_database("redis:6379,password=secret,db=0,socket_timeout=5", 2)
        == "redis:6379,password=secret,socket_timeout=5,db=2"
    )


def test_reader_holds_a_through_mid_request_cutover():
    """Công bố B giữa lúc đọc A không đổi registry/slot của request đang chạy."""
    state = [release()]
    cache = ReleaseCache(lambda: state[0])
    leases = Leases()

    def read(**kwargs):
        """Giả lập B vừa công bố giữa một lần đọc batch A."""
        state[0] = release("b", 2)
        cache.current(force=True)
        assert "a" in leases.active
        return FeatureResult({"seller_sku": ["sku"], "labeled_count_90d": [1000]})

    factory = Mock(return_value=SimpleNamespace(get_online_features=read))
    store = PublishedFeatureStore(Mock(), cache, leases, factory)
    result = store.get_online_features(
        features=["sku_return_daily:labeled_count_90d"], entity_rows=[{"seller_sku": "sku"}]
    )
    assert result.release.release_id == "a"
    assert result.to_dict()["labeled_count_90d"] == [1000]
    assert not leases.active
    factory.assert_called_once_with(release())


def test_stale_cache_cannot_read_reused_slot():
    """Generation fence buộc refresh sang B, không đọc A đã trở thành chỗ nạp C."""
    load = Mock(side_effect=[release(), release("b", 2)])
    cache, leases = ReleaseCache(load), Leases()
    cache.current()
    leases.retired.add("a")
    factory = Mock(
        return_value=SimpleNamespace(
            get_online_features=lambda **_: FeatureResult({"seller_sku": ["sku"]})
        )
    )
    store = PublishedFeatureStore(Mock(), cache, leases, factory)
    result = store.get_online_features(
        features=["sku_return_daily:labeled_count_90d"], entity_rows=[{"seller_sku": "sku"}]
    )
    assert result.release.release_id == "b"
    factory.assert_called_once_with(release("b", 2))


def test_expired_read_discards_result_and_stream_is_independent():
    """Batch quá hạn không rò dữ liệu; request chỉ stream không cần catalog hoạt động."""
    leases = Leases()
    leases.expired = True
    stream = Mock()
    stream.get_online_features.return_value = FeatureResult(
        {"seller_sku": ["sku"], "approved_returns_24h": [0]}
    )

    def factory(_):
        """Store batch giả trả cùng định danh SKU."""
        return SimpleNamespace(
            get_online_features=lambda **_: FeatureResult({"seller_sku": ["sku"]})
        )

    store = PublishedFeatureStore(stream, ReleaseCache(lambda: release()), leases, factory)
    with pytest.raises(FeatureDataUnavailable):
        store.get_online_features(
            features=["sku_return_daily:labeled_count_90d"], entity_rows=[{"seller_sku": "sku"}]
        )
    store.cache = ReleaseCache(Mock(side_effect=OSError("s3 down")))
    result = store.get_online_features(
        features=["sku_return_24h:approved_returns_24h"], entity_rows=[{"seller_sku": "sku"}]
    )
    assert result.to_dict()["approved_returns_24h"] == [0]


def catalog():
    """Catalog đang công bố A; write được theo dõi để chứng minh lỗi không đổi con trỏ."""
    result = Mock()
    result.pointer.return_value = (
        {"release_id": "a", "manifest_uri": "s3://test/a/manifest.json"},
        '"etag-a"',
    )
    result.read_json.return_value = (document(), '"manifest-etag"')
    return result


def test_failed_candidate_keeps_current_release():
    """Nạp/đối soát lỗi không được gọi publish và không đụng slot active."""
    source, leases = catalog(), Leases()
    with pytest.raises(ValueError, match="mismatch"):
        publish_candidate(source, leases, Mock(side_effect=ValueError("mismatch")))
    source.publish.assert_not_called()
    assert leases.prepared[0][0] == 2


def test_successful_candidate_is_published_with_expected_etag():
    """B chỉ được công bố sau build/validate; CAS tham chiếu đúng bản A đã đọc."""
    source, leases = catalog(), Leases()
    current = publish_candidate(source, leases, lambda db, name: document(name, db))
    assert current.database == 2
    assert source.publish.call_args.args[1] == '"etag-a"'
    assert source.publish.call_args.args[0]["validation"] == "passed"


def test_live_reader_prevents_slot_reuse():
    """Chặn ngay trước build: không xóa slot rồi mới nhận ra có reader."""
    source, leases, build = catalog(), Leases(), Mock()
    leases.active.add("old-b")
    with pytest.raises(PublicationBusy):
        publish_candidate(source, leases, build)
    build.assert_not_called()
    source.publish.assert_not_called()


@pytest.mark.parametrize("full_reload", [False, True])
def test_explicit_recovery_bypasses_active_empty_guard(full_reload):
    """Giữ quyền human full_reload đã chốt, không tự chữa bản active mất sạch."""
    source, leases = catalog(), Leases()
    check = Mock(side_effect=ValueError("active empty"))
    if full_reload:
        publish_candidate(
            source,
            leases,
            lambda db, name: document(name, db),
            full_reload=True,
            check_active=check,
        )
        check.assert_not_called()
    else:
        with pytest.raises(ValueError, match="active empty"):
            publish_candidate(source, leases, Mock(), check_active=check)
        source.publish.assert_not_called()


def snapshots():
    """Hai SKU, hai thời điểm; đủ provenance để so đúng bản."""
    return pd.DataFrame(
        [
            {
                "seller_sku": sku,
                "event_timestamp": stamp,
                "labeled_count_90d": count,
                "positive_count_90d": 1,
                "return_or_refund_rate_90d": 1 / count,
                "dataset_version": "v1",
                "generation_id": "g1",
            }
            for sku, stamp, count in [
                ("a", "2026-06-01", 10),
                ("a", "2026-06-02", 20),
                ("b", "2026-06-01", 5),
            ]
        ]
    )


def test_expected_snapshot_uses_each_sku_as_of_cutoff():
    """Không chọn toàn bộ file mới nhất hoặc feature tương lai."""
    expected = latest_expected(snapshots(), "2026-06-01T00:00:00Z")
    assert expected["seller_sku"].tolist() == ["a", "b"]
    assert expected["labeled_count_90d"].tolist() == [10, 5]


def test_duplicate_snapshot_identity_is_rejected():
    """Khóa SKU/timestamp trùng khiến snapshot không xác định duy nhất."""
    frame = snapshots()
    with pytest.raises(ValueError, match="duplicate"):
        latest_expected(pd.concat([frame, frame.iloc[:1]]), "2026-06-30")


def test_reconciliation_checks_values_and_provenance_not_just_counts():
    """Cùng số dòng nhưng sai số liệu/provenance phải fail; nullable hợp lệ vẫn qua."""
    expected = latest_expected(snapshots(), "2026-06-30")
    columns = {name: expected[name].tolist() for name in ["seller_sku", *BATCH_FIELDS]}
    store = Mock()
    store.get_online_features.return_value = FeatureResult(columns)
    reconcile(store, expected)
    columns["labeled_count_90d"] = list(reversed(columns["labeled_count_90d"]))
    with pytest.raises(ValueError, match="mismatch"):
        reconcile(store, expected)


def test_mixed_offline_generations_are_rejected():
    """Không công bố các SKU lấy từ hai lần sinh dữ liệu khác nhau."""
    frame = snapshots()
    frame.loc[0, "generation_id"] = "other-generation"
    with pytest.raises(ValueError, match="mixes"):
        latest_expected(frame, "2026-06-30")


def test_bootstrap_uses_first_slot_without_old_registry():
    """Lần đầu không tự coi thiếu active pointer là mất dữ liệu."""
    source, leases = catalog(), Leases()
    source.pointer.return_value = (None, None)
    result = publish_candidate(source, leases, lambda db, name: document(name, db))
    assert result.database == 1
    source.read_json.assert_not_called()


def test_invalid_candidate_proof_cannot_publish():
    """Build trả proof chưa đạt không được chuyển con trỏ."""
    source = catalog()

    def build(db, name):
        """Giả lập helper trả nhầm trạng thái kiểm tra."""
        return {**document(name, db), "validation": "failed"}

    with pytest.raises(FeatureDataUnavailable):
        publish_candidate(source, Leases(), build)
    source.publish.assert_not_called()


class MemoryS3:
    """S3 giả giữ ETag/conditional put, không chạm mạng hay credential."""

    def __init__(self):
        """Kho object trống cho bootstrap."""
        self.objects = {}

    def get_object(self, Bucket, Key):
        """Thiếu object chỉ trả NoSuchKey, giống S3 thật."""
        from io import BytesIO

        from botocore.exceptions import ClientError

        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        body, etag = self.objects[Bucket, Key]
        return {"Body": BytesIO(body), "ETag": etag}

    def put_object(self, Bucket, Key, Body, ContentType, **conditions):
        """CAS sai hoặc ghi đè manifest bất biến đều phải bị từ chối."""
        from botocore.exceptions import ClientError

        old = self.objects.get((Bucket, Key))
        if (conditions.get("IfNoneMatch") == "*" and old is not None) or (
            "IfMatch" in conditions and (old is None or old[1] != conditions["IfMatch"])
        ):
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[Bucket, Key] = (Body, f'"etag-{len(self.objects)}"')


def test_catalog_keeps_old_pointer_on_conflicting_publish():
    """Không ghi đè con trỏ mới hơn bằng một publisher đang giữ ETag cũ."""
    from botocore.exceptions import ClientError

    from feature_repo.publication import S3Catalog

    source = S3Catalog("s3://test/publications", MemoryS3())
    assert source.pointer() == (None, None)
    source.publish(document(), None)
    assert source.current().release_id == "a"
    with pytest.raises(ClientError):
        source.publish(document("b", 2), '"stale-etag"')
    assert source.current().release_id == "a"


@pytest.mark.parametrize("corrupt", [False, True])
def test_publisher_command_validates_before_switching_active(monkeypatch, tmp_path, corrupt):
    """Chạy nguyên command với transport giả: validation lỗi giữ A và dọn candidate, không flush active."""
    from feature_repo import publish_batch as command
    from feature_repo.publication import S3Catalog

    source = S3Catalog("s3://test/publications", MemoryS3())
    source.publish(document(), None)
    expected = latest_expected(snapshots(), "2026-06-30")
    columns = {name: expected[name].tolist() for name in ["seller_sku", *BATCH_FIELDS]}
    if corrupt:
        columns["labeled_count_90d"][0] = 999
    online = Mock()
    online.get_online_features.return_value = FeatureResult(columns)
    redis = Mock()
    redis.dbsize.return_value = len(expected)
    clients = []

    def client(store, database=0):
        """Ghi nhận DB nào được yêu cầu; không có Redis thật trong unit test."""
        clients.append(database)
        return redis

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FEAST_REDIS", "redis:6379")
    monkeypatch.setenv("FEAST_PUBLICATION_ROOT", source.root)
    monkeypatch.setenv("FEAST_SKU_DAILY_PATH", "s3://test/offline")
    monkeypatch.setattr(command, "S3Catalog", lambda _: source)
    monkeypatch.setattr(command, "RedisLeases", lambda _: Leases())
    monkeypatch.setattr(command, "redis_client", client)
    monkeypatch.setattr(command, "make_store", Mock(return_value=online))
    monkeypatch.setattr(command, "snapshot_parquet", Mock())
    monkeypatch.setattr(command.subprocess, "run", Mock())
    monkeypatch.setattr(pd, "read_parquet", Mock(return_value=snapshots()))
    if corrupt:
        with pytest.raises(ValueError, match="mismatch"):
            command.main(["--end", "2026-06-30", "--full-reload"])
        assert source.current().release_id == "a"
        assert redis.flushdb.call_count == 2
    else:
        command.main(["--end", "2026-06-30", "--full-reload"])
        assert source.current().database == 2
        assert source.current().release_id != "a"
        assert redis.flushdb.call_count == 1
    assert clients == [0, 2]
