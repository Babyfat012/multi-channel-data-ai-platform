"""Công bố batch A/B: con trỏ S3, cache ngắn và lease đọc được bảo vệ trong Redis DB0."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

BATCH_VIEW = "sku_return_daily"
BATCH_FIELDS = (
    "labeled_count_90d",
    "positive_count_90d",
    "return_or_refund_rate_90d",
    "dataset_version",
    "generation_id",
)


class FeatureDataUnavailable(RuntimeError):
    """Không đủ căn cứ để phục vụ một bản batch đã công bố."""


class PublicationBusy(RuntimeError):
    """Writer khác hoặc lượt đọc cũ đang giữ slot; dừng để thử lại sau."""


def redis_database(connection, database):
    """Chọn DB theo cú pháp Feast, giữ host/auth và các tùy chọn khác."""
    parts = [p for p in connection.split(",") if not p.startswith("db=")]
    return ",".join([*parts, f"db={database}"])


@dataclass(frozen=True)
class Release:
    """Danh tính một bản đã đối soát, không đổi khi con trỏ active chuyển bản."""

    release_id: str
    database: int
    registry_uri: str
    offline_uri: str
    cutoff: str
    expected_skus: frozenset[str]

    @classmethod
    def from_document(cls, document):
        """Chỉ cho reader sử dụng manifest đã đạt đối soát ở slot batch hợp lệ."""
        if document.get("validation") != "passed" or document.get("database") not in (1, 2):
            raise FeatureDataUnavailable("batch manifest is not validated")
        if not document.get("release_id") or not document.get("registry_uri"):
            raise FeatureDataUnavailable("batch manifest has no identity/registry")
        skus = document.get("expected_skus")
        if (
            not isinstance(skus, list)
            or not skus
            or any(not isinstance(s, str) or not s for s in skus)
        ):
            raise FeatureDataUnavailable("batch manifest has no expected SKU set")
        return cls(
            document["release_id"],
            document["database"],
            document["registry_uri"],
            document["offline_uri"],
            document["cutoff"],
            frozenset(skus),
        )


def s3_location(uri):
    """Đọc bucket/key S3, không nhận đường dẫn không xác định."""
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError("publication requires an s3://bucket/key path")
    return parsed.netloc, parsed.path.lstrip("/")


class S3Catalog:
    """Manifest bất biến và active pointer có conditional write để tránh ghi đè công bố khác."""

    def __init__(self, root, client=None):
        """Dùng IAM role trên AWS hoặc endpoint/credential local đã cấu hình."""
        if client is None:
            import boto3

            client = boto3.client("s3", endpoint_url=os.environ.get("FEAST_S3_ENDPOINT_URL"))
        self.root, self.client = root.rstrip("/"), client

    def read_json(self, uri):
        """Đọc object JSON cùng ETag để dùng khi công bố có điều kiện."""
        bucket, key = s3_location(uri)
        response = self.client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read()), response["ETag"]

    def pointer(self):
        """Chỉ NoSuchKey là bootstrap; lỗi quyền/mạng phải được truyền lên."""
        from botocore.exceptions import ClientError

        try:
            return self.read_json(f"{self.root}/active.json")
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None, None
            raise

    def current(self):
        """Giải con trỏ thành manifest, kiểm danh tính trùng khớp."""
        pointer, _ = self.pointer()
        if pointer is None:
            raise FeatureDataUnavailable("batch has no published release")
        document, _ = self.read_json(pointer["manifest_uri"])
        release = Release.from_document(document)
        if release.release_id != pointer["release_id"]:
            raise FeatureDataUnavailable("batch pointer/manifest mismatch")
        return release

    def write_json(self, uri, document, **conditions):
        """Ghi nguyên object; caller chọn điều kiện IfMatch/IfNoneMatch."""
        bucket, key = s3_location(uri)
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(document, allow_nan=False).encode(),
            ContentType="application/json",
            **conditions,
        )

    def publish(self, document, previous_etag):
        """Lưu proof trước, chỉ đổi active sau khi manifest hợp lệ đã tồn tại."""
        release = Release.from_document(document)
        uri = f"{self.root}/releases/{release.release_id}/manifest.json"
        self.write_json(uri, document, IfNoneMatch="*")
        condition = {"IfNoneMatch": "*"} if previous_etag is None else {"IfMatch": previous_etag}
        self.write_json(
            f"{self.root}/active.json",
            {"release_id": release.release_id, "manifest_uri": uri},
            **condition,
        )


class ReleaseCache:
    """Cache active tối đa TTL; lỗi refresh không được giữ bản cũ vô thời hạn."""

    def __init__(self, load, ttl=5, clock=time.monotonic):
        """Inject loader/clock để kiểm cutover mà không ngủ trong test."""
        if ttl <= 0:
            raise ValueError("cache TTL must be positive")
        self.load, self.ttl, self.clock = load, ttl, clock
        self._release, self._until = None, 0
        self._lock = threading.Lock()

    def current(self, force=False):
        """Trả đối tượng bất biến: request cũ giữ A dù cache đã chuyển B."""
        with self._lock:
            if force or self._release is None or self.clock() >= self._until:
                release = self.load()
                self._release, self._until = release, self.clock() + self.ttl
            return self._release


# Đồng hồ Redis tránh dùng thời gian khác nhau giữa các Pod/EC2.
_NOW = "local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000); "
_ACQUIRE = (
    _NOW
    + """
if redis.call('HGET',KEYS[1],'release_id')~=ARGV[1] or
   redis.call('HGET',KEYS[1],'status')~='ready' then return 0 end
redis.call('ZREMRANGEBYSCORE',KEYS[2],'-inf',now)
redis.call('ZADD',KEYS[2],now+tonumber(ARGV[3]),ARGV[2]); return 1
"""
)
_FINISH = (
    _NOW
    + """
local expiry=redis.call('ZSCORE',KEYS[2],ARGV[2])
local valid=expiry and tonumber(expiry)>now and
 redis.call('HGET',KEYS[1],'release_id')==ARGV[1] and
 redis.call('HGET',KEYS[1],'status')=='ready'
redis.call('ZREM',KEYS[2],ARGV[2]); if valid then return 1 else return 0 end
"""
)
_PREPARE = (
    _NOW
    + """
if redis.call('GET',KEYS[3])~=ARGV[2] then return 0 end
redis.call('ZREMRANGEBYSCORE',KEYS[2],'-inf',now)
if redis.call('ZCARD',KEYS[2])>0 then return 0 end
redis.call('HSET',KEYS[1],'release_id',ARGV[1],'status','loading'); return 1
"""
)
_READY = """
if redis.call('GET',KEYS[2])~=ARGV[2] or
 redis.call('HGET',KEYS[1],'release_id')~=ARGV[1] then return 0 end
redis.call('HSET',KEYS[1],'status','ready'); return 1
"""
_UNLOCK = (
    "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end; return 0"
)


class RedisLeases:
    """Reader lease có hạn và generation fence ngăn tái dùng slot khi request cũ còn đọc."""

    def __init__(self, client, prefix="platform:batch-publication", seconds=30):
        """Control nằm DB0; không ghi control vào DB1/2 sẽ được dùng lại."""
        self.client, self.prefix, self.seconds = client, prefix, seconds
        self.writer_key = f"{prefix}:writer"

    def keys(self, database):
        """Cùng slot có trạng thái generation và tập lease riêng."""
        return [f"{self.prefix}:slot:{database}", f"{self.prefix}:readers:{database}"]

    def acquire(self, release):
        """Trả token nếu generation còn ready; con trỏ cache cũ bị fence sẽ phải refresh."""
        token = uuid.uuid4().hex
        ok = self.client.eval(
            _ACQUIRE,
            2,
            *self.keys(release.database),
            release.release_id,
            token,
            int(self.seconds * 1000),
        )
        return token if ok else None

    def finish(self, release, token):
        """Chỉ được trả dữ liệu nếu lease vẫn còn hiệu lực khi đọc xong."""
        if not self.client.eval(
            _FINISH, 2, *self.keys(release.database), release.release_id, token
        ):
            raise FeatureDataUnavailable("batch read lease expired; result discarded")

    def cancel(self, release, token):
        """Dọn lease khi đọc lỗi; nếu dọn thất bại, lease tự hết hạn."""
        self.client.zrem(self.keys(release.database)[1], token)

    @contextmanager
    def writer(self):
        """Một writer, không tự hết hạn: crash cần người vận hành xác minh trước khi mở khóa."""
        owner = uuid.uuid4().hex
        if not self.client.set(self.writer_key, owner, nx=True):
            raise PublicationBusy("batch publisher locked; inspect previous run before unlocking")
        try:
            yield owner
        finally:
            self.client.eval(_UNLOCK, 1, self.writer_key, owner)

    def prepare(self, database, release_id, owner):
        """Đóng nhận reader generation cũ trước khi writer được xóa/nạp lại slot."""
        if not self.client.eval(
            _PREPARE,
            3,
            *self.keys(database),
            self.writer_key,
            release_id,
            owner,
        ):
            raise PublicationBusy("batch slot still has readers or writer lost ownership")

    def ready(self, database, release_id, owner):
        """Đánh dấu generation đã đối soát, trước conditional publish S3."""
        if not self.client.eval(
            _READY,
            2,
            self.keys(database)[0],
            self.writer_key,
            release_id,
            owner,
        ):
            raise PublicationBusy("batch writer lost ownership before publish")


def make_store(repo, connection, registry=None):
    """Tạo config riêng cho từng store, không đổi os.environ dùng chung giữa request."""
    import yaml
    from feast import FeatureStore
    from feast.repo_config import RepoConfig

    config = yaml.safe_load(os.path.expandvars((Path(repo) / "feature_store.yaml").read_text()))
    config["online_store"]["connection_string"] = connection
    if registry is not None:
        config["registry"] = registry
    return FeatureStore(config=RepoConfig(**config))


def redis_client(store, database=0):
    """Dùng parser Redis của Feast đã pin, giữ auth và chọn đúng logical DB."""
    from feast.infra.online_stores.redis import RedisOnlineStore

    config = store.config.online_store.model_copy(deep=True)
    config.connection_string = redis_database(config.connection_string, database)
    return RedisOnlineStore()._get_client(config)


class FeatureResult:
    """Kết quả ghép batch đã pin và stream, giữ giao diện mà API/predictor đang dùng."""

    def __init__(self, columns, release=None):
        """Đính kèm bản đã đọc để response không nhầm với bản vừa được công bố sau đó."""
        self.columns, self.release = columns, release

    def to_dict(self):
        """Giữ hình dạng cột→list của OnlineResponse Feast."""
        return self.columns

    def to_df(self):
        """Predictor cần DataFrame, chỉ import pandas khi dùng."""
        import pandas as pd

        return pd.DataFrame(self.columns)


class PublishedFeatureStore:
    """Reader batch dùng active trên S3; stream tiếp tục DB0 với registry cũ."""

    def __init__(self, stream, cache, leases, batch_factory):
        """Transport/config được inject để unit test không cần Feast/AWS."""
        self.stream, self.cache, self.leases, self.batch_factory = (
            stream,
            cache,
            leases,
            batch_factory,
        )
        self._stores, self._lock = {}, threading.Lock()

    def _pin(self):
        """Cached generation bị thu hồi thì refresh active một lần, không đọc slot đang nạp."""
        for force in (False, True):
            release = self.cache.current(force=force)
            token = self.leases.acquire(release)
            if token is not None:
                return release, token
        raise FeatureDataUnavailable("published batch is unavailable or being reclaimed")

    def get_online_features(self, features, entity_rows, **kwargs):
        """Ghim một bản cho toàn bộ nhóm batch trong lần đọc; lease bao quanh việc lấy dữ liệu."""
        batch = [f for f in features if f.split(":", 1)[0] == BATCH_VIEW]
        stream = [f for f in features if f not in batch]
        columns, release = {}, None
        if batch:
            token = None
            try:
                release, token = self._pin()
                with self._lock:
                    if release.release_id not in self._stores:
                        self._stores = {release.release_id: self.batch_factory(release)}
                    store = self._stores[release.release_id]
                columns.update(
                    store.get_online_features(
                        features=batch,
                        entity_rows=entity_rows,
                        **kwargs,
                    ).to_dict()
                )
                if columns.get("seller_sku") != [row["seller_sku"] for row in entity_rows]:
                    raise FeatureDataUnavailable("batch entity identity/order mismatch")
                self.leases.finish(release, token)
                token = None
            except Exception as exc:
                if token is not None:
                    with suppress(Exception):
                        self.leases.cancel(release, token)
                raise FeatureDataUnavailable("batch feature data unavailable") from exc
        if stream:
            result = self.stream.get_online_features(
                features=stream, entity_rows=entity_rows, **kwargs
            )
            values = result.to_dict()
            if values.get("seller_sku") != [row["seller_sku"] for row in entity_rows]:
                raise FeatureDataUnavailable("stream entity identity/order mismatch")
            columns.update(values)
        return FeatureResult(columns, release)

    async def get_online_features_async(self, **kwargs):
        """Giữ I/O S3/Redis đồng bộ ngoài event loop FastAPI."""
        import asyncio

        return await asyncio.to_thread(self.get_online_features, **kwargs)


def load_published_store(repo):
    """Opt-in qua FEAST_PUBLICATION_ROOT; reader cũ giữ nguyên đến khi migration được bật."""
    connection = os.environ["FEAST_REDIS"]
    stream = make_store(repo, redis_database(connection, 0))
    catalog = S3Catalog(os.environ["FEAST_PUBLICATION_ROOT"])
    return PublishedFeatureStore(
        stream,
        ReleaseCache(catalog.current),
        RedisLeases(redis_client(stream, 0)),
        lambda release: make_store(
            repo,
            redis_database(connection, release.database),
            release.registry_uri,
        ),
    )
