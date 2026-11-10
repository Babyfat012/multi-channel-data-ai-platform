"""A/B batch publication: S3 pointer, short cache and guarded read leases in Redis DB0."""

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
    """Not enough grounds to serve a published batch release."""


class PublicationBusy(RuntimeError):
    """Another writer or an old read holds the slot; stop and retry later."""


def redis_database(connection, database):
    """Select the DB using Feast syntax, keeping host/auth and other options."""
    parts = [p for p in connection.split(",") if not p.startswith("db=")]
    return ",".join([*parts, f"db={database}"])


@dataclass(frozen=True)
class Release:
    """Identity of a reconciled release; does not change when the active pointer moves to another release."""

    release_id: str
    database: int
    registry_uri: str
    offline_uri: str
    cutoff: str
    expected_skus: frozenset[str]

    @classmethod
    def from_document(cls, document):
        """Let readers use only a manifest that passed reconciliation in a valid batch slot."""
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
    """Parse the S3 bucket/key; reject undetermined paths."""
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError("publication requires an s3://bucket/key path")
    return parsed.netloc, parsed.path.lstrip("/")


class S3Catalog:
    """Immutable manifests and an active pointer with conditional writes to avoid overwriting another publication."""

    def __init__(self, root, client=None):
        """Use the IAM role on AWS or the configured local endpoint/credentials."""
        if client is None:
            import boto3

            client = boto3.client("s3", endpoint_url=os.environ.get("FEAST_S3_ENDPOINT_URL"))
        self.root, self.client = root.rstrip("/"), client

    def read_json(self, uri):
        """Read a JSON object with its ETag for conditional publication."""
        bucket, key = s3_location(uri)
        response = self.client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read()), response["ETag"]

    def pointer(self):
        """Only NoSuchKey means bootstrap; permission/network errors must propagate."""
        from botocore.exceptions import ClientError

        try:
            return self.read_json(f"{self.root}/active.json")
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None, None
            raise

    def current(self):
        """Resolve the pointer to a manifest and check that the identities match."""
        pointer, _ = self.pointer()
        if pointer is None:
            raise FeatureDataUnavailable("batch has no published release")
        document, _ = self.read_json(pointer["manifest_uri"])
        release = Release.from_document(document)
        if release.release_id != pointer["release_id"]:
            raise FeatureDataUnavailable("batch pointer/manifest mismatch")
        return release

    def write_json(self, uri, document, **conditions):
        """Write the whole object; the caller chooses the IfMatch/IfNoneMatch condition."""
        bucket, key = s3_location(uri)
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(document, allow_nan=False).encode(),
            ContentType="application/json",
            **conditions,
        )

    def publish(self, document, previous_etag):
        """Store the proof first; switch active only once a valid manifest exists."""
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
    """Cache active for at most TTL; a failed refresh must not keep the old release indefinitely."""

    def __init__(self, load, ttl=5, clock=time.monotonic):
        """Inject loader/clock to test cutover without sleeping in tests."""
        if ttl <= 0:
            raise ValueError("cache TTL must be positive")
        self.load, self.ttl, self.clock = load, ttl, clock
        self._release, self._until = None, 0
        self._lock = threading.Lock()

    def current(self, force=False):
        """Return an immutable object: an old request keeps A even after the cache moved to B."""
        with self._lock:
            if force or self._release is None or self.clock() >= self._until:
                release = self.load()
                self._release, self._until = release, self.clock() + self.ttl
            return self._release


# The Redis clock avoids differing times across Pods/EC2.
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
    """Expiring reader leases and a generation fence prevent slot reuse while old requests are still reading."""

    def __init__(self, client, prefix="platform:batch-publication", seconds=30):
        """Control lives in DB0; never write control into DB1/2, which get reused."""
        self.client, self.prefix, self.seconds = client, prefix, seconds
        self.writer_key = f"{prefix}:writer"

    def keys(self, database):
        """Each slot has its own generation state and lease set."""
        return [f"{self.prefix}:slot:{database}", f"{self.prefix}:readers:{database}"]

    def acquire(self, release):
        """Return a token if the generation is still ready; a fenced stale cached pointer must refresh."""
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
        """Data may be returned only if the lease is still valid when the read finishes."""
        if not self.client.eval(
            _FINISH, 2, *self.keys(release.database), release.release_id, token
        ):
            raise FeatureDataUnavailable("batch read lease expired; result discarded")

    def cancel(self, release, token):
        """Clean up the lease when a read fails; if cleanup fails, the lease expires on its own."""
        self.client.zrem(self.keys(release.database)[1], token)

    @contextmanager
    def writer(self):
        """One writer, no auto-expiry: after a crash an operator must verify before unlocking."""
        owner = uuid.uuid4().hex
        if not self.client.set(self.writer_key, owner, nx=True):
            raise PublicationBusy("batch publisher locked; inspect previous run before unlocking")
        try:
            yield owner
        finally:
            self.client.eval(_UNLOCK, 1, self.writer_key, owner)

    def prepare(self, database, release_id, owner):
        """Stop admitting readers of the old generation before the writer may clear/reload the slot."""
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
        """Mark the generation as reconciled, before the conditional S3 publish."""
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
    """Build a separate config per store without changing the os.environ shared between requests."""
    import yaml
    from feast import FeatureStore
    from feast.repo_config import RepoConfig

    config = yaml.safe_load(os.path.expandvars((Path(repo) / "feature_store.yaml").read_text()))
    config["online_store"]["connection_string"] = connection
    if registry is not None:
        config["registry"] = registry
    return FeatureStore(config=RepoConfig(**config))


def redis_client(store, database=0):
    """Use the pinned Feast Redis parser, keeping auth and selecting the right logical DB."""
    from feast.infra.online_stores.redis import RedisOnlineStore

    config = store.config.online_store.model_copy(deep=True)
    config.connection_string = redis_database(config.connection_string, database)
    return RedisOnlineStore()._get_client(config)


class FeatureResult:
    """Combined result of the pinned batch and stream, keeping the interface the API/predictor use."""

    def __init__(self, columns, release=None):
        """Attach the release that was read so the response is not confused with one published afterwards."""
        self.columns, self.release = columns, release

    def to_dict(self):
        """Keep the column→list shape of Feast's OnlineResponse."""
        return self.columns

    def to_df(self):
        """The predictor needs a DataFrame; import pandas only when used."""
        import pandas as pd

        return pd.DataFrame(self.columns)


class PublishedFeatureStore:
    """The batch reader uses the active release on S3; stream keeps DB0 with the old registry."""

    def __init__(self, stream, cache, leases, batch_factory):
        """Transport/config are injected so unit tests need no Feast/AWS."""
        self.stream, self.cache, self.leases, self.batch_factory = (
            stream,
            cache,
            leases,
            batch_factory,
        )
        self._stores, self._lock = {}, threading.Lock()

    def _pin(self):
        """If the cached generation was reclaimed, refresh active once; never read a slot being loaded."""
        for force in (False, True):
            release = self.cache.current(force=force)
            token = self.leases.acquire(release)
            if token is not None:
                return release, token
        raise FeatureDataUnavailable("published batch is unavailable or being reclaimed")

    def get_online_features(self, features, entity_rows, **kwargs):
        """Pin one release for the whole batch group in a read; the lease wraps the data fetch."""
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
        """Keep synchronous S3/Redis I/O off the FastAPI event loop."""
        import asyncio

        return await asyncio.to_thread(self.get_online_features, **kwargs)


def load_published_store(repo):
    """Opt-in via FEAST_PUBLICATION_ROOT; the old reader stays as is until the migration is enabled."""
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
