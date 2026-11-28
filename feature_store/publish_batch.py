"""Load the batch into a non-serving slot, reconcile, then publish conditionally; never overwrite the active release."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from contextlib import suppress
from pathlib import Path

if __package__:
    from .publication import (
        BATCH_FIELDS,
        BATCH_VIEW,
        RedisLeases,
        Release,
        S3Catalog,
        make_store,
        redis_client,
        redis_database,
        s3_location,
    )
else:
    from publication import (
        BATCH_FIELDS,
        BATCH_VIEW,
        RedisLeases,
        Release,
        S3Catalog,
        make_store,
        redis_client,
        redis_database,
        s3_location,
    )


def latest_expected(frame, end):
    """Pick the latest snapshot per SKU up to end; duplicate or missing keys/timestamps must fail."""
    import pandas as pd

    required = ["seller_sku", "event_timestamp", *BATCH_FIELDS]
    if not set(required).issubset(frame.columns):
        raise ValueError("offline snapshot missing required columns")
    frame = frame[required].copy()
    frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
    if frame[["seller_sku", "event_timestamp"]].isna().any().any():
        raise ValueError("offline snapshot has null identity/timestamp")
    if frame.duplicated(["seller_sku", "event_timestamp"]).any():
        raise ValueError("offline snapshot has duplicate SKU/timestamp")
    if (frame[["dataset_version", "generation_id"]].nunique() != 1).any():
        raise ValueError("offline snapshot mixes dataset/generation versions")
    frame = frame[frame["event_timestamp"] <= pd.to_datetime(end, utc=True)]
    frame = frame.sort_values(["seller_sku", "event_timestamp"]).drop_duplicates(
        "seller_sku", keep="last"
    )
    if (
        frame.empty
        or frame[["labeled_count_90d", "positive_count_90d", "dataset_version", "generation_id"]]
        .isna()
        .any()
        .any()
    ):
        raise ValueError("offline snapshot empty or missing mandatory values")
    counts, positives = frame["labeled_count_90d"], frame["positive_count_90d"]
    if ((counts < 0) | (positives < 0) | (positives > counts)).any():
        raise ValueError("offline snapshot has invalid counts")
    rates = frame["return_or_refund_rate_90d"]
    if ((counts > 0) & rates.isna()).any() or ((rates < 0) | (rates > 1)).any():
        raise ValueError("offline snapshot has invalid/missing return rate")
    return frame.reset_index(drop=True)


def reconcile(store, expected, batch_size=500):
    """Compare every SKU/value, including provenance; a valid NULL matches only NULL, not just a count comparison."""
    import pandas as pd

    for offset in range(0, len(expected), batch_size):
        chunk = expected.iloc[offset : offset + batch_size]
        skus = chunk["seller_sku"].tolist()
        actual = store.get_online_features(
            features=[f"{BATCH_VIEW}:{field}" for field in BATCH_FIELDS],
            entity_rows=[{"seller_sku": sku} for sku in skus],
        ).to_dict()
        if actual.get("seller_sku") != skus:
            raise ValueError("online SKU identity/order mismatch")
        for field in BATCH_FIELDS:
            if field not in actual or len(actual[field]) != len(skus):
                raise ValueError(f"online column missing/short: {field}")
            for sku, want, got in zip(skus, chunk[field], actual[field], strict=True):
                if pd.isna(want):
                    equal = pd.isna(got)
                elif pd.isna(got):
                    equal = False
                elif field == "return_or_refund_rate_90d":
                    equal = math.isclose(float(want), float(got), rel_tol=1e-7, abs_tol=1e-9)
                else:
                    equal = want == got
                if not equal:
                    raise ValueError(f"online/offline mismatch: sku={sku}, field={field}")


def publish_candidate(catalog, leases, build, *, full_reload=False, check_active=None):
    """Transport-independent orchestration: a build/reconciliation failure must not change the active pointer."""
    with leases.writer() as owner:
        pointer, etag = catalog.pointer()
        current = None
        if pointer is not None:
            document, _ = catalog.read_json(pointer["manifest_uri"])
            current = Release.from_document(document)
            if current.release_id != pointer["release_id"]:
                raise ValueError("active pointer/manifest mismatch")
        if current is not None and not full_reload and check_active is not None:
            check_active(current)
        database = 1 if current is None or current.database == 2 else 2
        release_id = uuid.uuid4().hex
        leases.prepare(database, release_id, owner)
        # build must return the manifest after reconciling, and must not publish on its own.
        document = build(database, release_id)
        release = Release.from_document(document)
        if release.release_id != release_id or release.database != database:
            raise ValueError("candidate identity/slot mismatch")
        leases.ready(database, release_id, owner)
        catalog.publish(document, etag)
        return release


def snapshot_parquet(catalog, source, destination):
    """Copy each Parquet file with its source ETag, keeping the release's offline data immutable on S3."""
    bucket, prefix = s3_location(source)
    target_bucket, target_prefix = s3_location(destination)
    pages = catalog.client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix.rstrip("/") + "/"
    )
    copied = 0
    for page in pages:
        for item in page.get("Contents", []):
            if not item["Key"].endswith(".parquet"):
                continue
            relative = item["Key"][len(prefix.rstrip("/")) + 1 :]
            catalog.client.copy_object(
                Bucket=target_bucket,
                Key=f"{target_prefix}/{relative}",
                CopySource={"Bucket": bucket, "Key": item["Key"]},
                CopySourceIfMatch=item["ETag"],
            )
            copied += 1
    if not copied:
        raise ValueError("no offline Parquet files to publish")


def main(argv=None):
    """Run by the DAG inside the Feast venv; enabled on cloud only by explicit opt-in."""
    import pandas as pd
    import s3fs

    if __package__:
        from .check_online_store import check_incremental_online_store
    else:
        from check_online_store import check_incremental_online_store

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--end", required=True)
    parser.add_argument("--full-reload", action="store_true")
    args = parser.parse_args(argv)
    repo = Path.cwd()
    connection = os.environ["FEAST_REDIS"]
    catalog = S3Catalog(os.environ["FEAST_PUBLICATION_ROOT"])
    stream = make_store(repo, redis_database(connection, 0))
    control = redis_client(stream, 0)
    leases = RedisLeases(control)

    def check_active(release):
        """Do not self-heal a fully lost active release unless full_reload was requested."""
        store = make_store(repo, redis_database(connection, release.database), release.registry_uri)
        client = redis_client(store, release.database)
        try:
            check_incremental_online_store(store, client)
        finally:
            client.close()

    def build(database, release_id):
        """Clear only a fenced slot, load from a dedicated snapshot, then compare every value."""
        root = f"{catalog.root}/releases/{release_id}"
        offline, registry = f"{root}/sku_daily", f"{root}/registry.db"
        snapshot_parquet(catalog, os.environ["FEAST_SKU_DAILY_PATH"], offline)
        fs = s3fs.S3FileSystem(endpoint_url=os.environ.get("FEAST_S3_ENDPOINT_URL"))
        expected = latest_expected(
            pd.read_parquet(offline.removeprefix("s3://"), filesystem=fs), args.end
        )
        target_connection = redis_database(connection, database)
        env = {
            **os.environ,
            "FEAST_REDIS": target_connection,
            "FEAST_REGISTRY": registry,
            "FEAST_SKU_DAILY_PATH": offline,
        }
        with tempfile.TemporaryDirectory() as directory:
            target_repo = shutil.copytree(repo, Path(directory) / "repo")
            executable = str(Path(sys.executable).with_name("feast"))
            # Create a client for the right slot; the slot's old Redis data is cleared only once no reads remain.
            store = make_store(target_repo, target_connection, registry)
            client = redis_client(store, database)
            try:
                client.flushdb()
                for command in (
                    ["apply"],
                    ["materialize", "--views", BATCH_VIEW, "2000-01-01T00:00:00+00:00", args.end],
                ):
                    subprocess.run(
                        [executable, *command], cwd=target_repo, env=env, check=True, timeout=1800
                    )
                # Reload to pick up the just applied/materialized registry, not stale cached metadata.
                store = make_store(target_repo, target_connection, registry)
                reconcile(store, expected)
                if client.dbsize() != len(expected):
                    raise ValueError("online SKU key count differs from validated snapshot")
            except Exception:
                # The materialize subprocess has stopped when it returns an error/timeout. The slot is fenced, not active.
                # Clean up the failed candidate so it does not hold RAM or affect the old release.
                with suppress(Exception):
                    client.flushdb()
                raise
            finally:
                client.close()
        digest = hashlib.sha256(
            expected.to_json(orient="records", date_format="iso").encode()
        ).hexdigest()
        return {
            "release_id": release_id,
            "database": database,
            "registry_uri": registry,
            "offline_uri": offline,
            "cutoff": args.end,
            "validation": "passed",
            "expected_skus": expected["seller_sku"].tolist(),
            "snapshot_sha256": digest,
        }

    try:
        release = publish_candidate(
            catalog, leases, build, full_reload=args.full_reload, check_active=check_active
        )
        print(
            json.dumps(
                {
                    "published": release.release_id,
                    "database": release.database,
                    "cutoff": release.cutoff,
                }
            )
        )
    finally:
        control.close()


if __name__ == "__main__":
    main()
