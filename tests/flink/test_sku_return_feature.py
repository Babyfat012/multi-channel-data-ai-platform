"""Test the PyFlink stream feature job on a local mini cluster (needs Java + PyFlink + Feast: run in the image).

docker run --rm --network none --hostname localhost --tmpfs /tmp:exec -e HOME=/tmp -v "$PWD":/w:ro -w /w \
    platform-flink-ml:2.2.1 python -m pytest -q -p no:cacheprovider tests/flink
"""

import json
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flink_jobs/ml"))
import sku_return_feature as job  # noqa: E402

T0 = datetime(2026, 6, 27, 8, 0, tzinfo=UTC)


def event(sha, sku, at, event_type="approved", quantity=1):
    """Message like replay_return_events.py (effective_at in milliseconds, Z suffix)."""
    return json.dumps(
        {
            "record_sha256": sha,
            "effective_at": at.strftime("%Y-%m-%dT%H:%M:%S.") + f"{at.microsecond // 1000:03d}Z",
            "event_type": event_type,
            "quantity": quantity,
            "seller_sku": sku,
        }
    )


def oracle(raw):
    """Independent count (no Flink): dedup by sha, each window [end−24h, end) aligned to hours with messages."""
    seen, events = set(), []
    for line in raw:
        e = job.parse_event(line)
        if e and e[0] not in seen:
            seen.add(e[0])
            events.append(e)
    counts = Counter()
    for _, sku, ms in events:
        first_end = (ms // 3_600_000 + 1) * 3_600_000
        for k in range(24):
            counts[(sku, first_end + k * 3_600_000)] += 1
    return counts


def test_parse_event_keeps_only_valid_approved():
    """Only approved Returns, quantity > 0, all fields present; time truncated to milliseconds, UTC."""
    at = T0 + timedelta(minutes=5, microseconds=123_456)
    ms = int((T0 + timedelta(minutes=5, milliseconds=123)).timestamp() * 1000)
    assert job.parse_event(event("a", "S", at)) == ("a", "S", ms)
    assert job.parse_event(event("a", "S", at, event_type="requested")) is None
    assert job.parse_event(event("a", "S", at, quantity=0)) is None
    assert job.parse_event("not json") is None
    assert job.parse_event(json.dumps({"event_type": "approved", "quantity": 1})) is None


def test_windows_match_independent_count():
    """The job (dedup + HOP 24h/1h) yields exactly each window like the independent count, including duplicate and late messages."""
    from pyflink.datastream import StreamExecutionEnvironment

    raw = [
        event("a", "S1", T0 + timedelta(minutes=10)),
        event("a", "S1", T0 + timedelta(minutes=10)),  # duplicate replay
        event("b", "S1", T0 + timedelta(hours=3)),
        event("c", "S2", T0 + timedelta(hours=1, minutes=30)),
        event(
            "d", "S1", T0 + timedelta(hours=2, minutes=40)
        ),  # < 1h late relative to the previous message
        event("e", "S2", T0 + timedelta(hours=1), event_type="rejected"),
    ]
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    from pyflink.common import Types

    stream = env.from_collection(raw, type_info=Types.STRING())
    with job.build_windows(stream, assign_event_time=True).execute_and_collect() as results:
        got = Counter({(sku, end): count for sku, end, count in results})
    assert got == oracle(raw)
    assert got[("S1", int((T0 + timedelta(hours=1)).timestamp() * 1000))] == 1


def test_offline_parts_read_by_feast_point_in_time(tmp_path, monkeypatch):
    """Two Parquet batches in one directory ⇒ the Feast FileSource reads the whole directory, the PIT join takes the latest window before the timestamp."""
    from feast import Entity, FeatureStore, FeatureView, Field, FileSource, PushSource, ValueType
    from feast.repo_config import RepoConfig
    from feast.types import Int64

    offline = tmp_path / "sku_return_24h"
    offline.mkdir()
    end = lambda h: int((T0 + timedelta(hours=h)).timestamp() * 1000)  # noqa: E731
    job.write_offline(job.to_rows([("S1", end(1), 2), ("S1", end(2), 3)]), str(offline))
    job.write_offline(job.to_rows([("S1", end(3), 5)]), str(offline))
    assert len(list(offline.glob("part-*.parquet"))) == 2

    store = FeatureStore(
        config=RepoConfig(
            project="t",
            provider="local",
            registry=str(tmp_path / "registry.db"),
            online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
            offline_store={"type": "file"},
            entity_key_serialization_version=3,
        )
    )
    sku = Entity(name="sku", join_keys=["seller_sku"], value_type=ValueType.STRING)
    push = PushSource(
        name="sku_return_24h_push",
        batch_source=FileSource(path=str(offline), timestamp_field="event_timestamp"),
    )
    view = FeatureView(
        name="sku_return_24h",
        entities=[sku],
        ttl=timedelta(days=1),
        schema=[Field(name="approved_returns_24h", dtype=Int64)],
        source=push,
    )
    store.apply([sku, push, view])
    import pandas as pd

    entity_df = pd.DataFrame(
        {
            "seller_sku": ["S1", "S1"],
            "event_timestamp": [T0 + timedelta(hours=2, minutes=30), T0 + timedelta(hours=9)],
        }
    )
    out = store.get_historical_features(
        entity_df=entity_df, features=["sku_return_24h:approved_returns_24h"]
    ).to_df()
    assert sorted(out["approved_returns_24h"].tolist()) == [3, 5]


def test_online_push_read_back_by_feast(tmp_path, monkeypatch):
    """Online branch: Feast push ONLINE then get_online_features (how the Feature API reads) returns the latest window value."""
    from feast import Entity, FeatureStore, FeatureView, Field, FileSource, PushSource, ValueType
    from feast.repo_config import RepoConfig
    from feast.types import Int64

    (tmp_path / "off").mkdir()
    store = FeatureStore(
        config=RepoConfig(
            project="t",
            provider="local",
            registry=str(tmp_path / "registry.db"),
            online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
            offline_store={"type": "file"},
            entity_key_serialization_version=3,
        )
    )
    sku = Entity(name="sku", join_keys=["seller_sku"], value_type=ValueType.STRING)
    push = PushSource(
        name="sku_return_24h_push",
        batch_source=FileSource(path=str(tmp_path / "off"), timestamp_field="event_timestamp"),
    )
    view = FeatureView(
        name="sku_return_24h",
        entities=[sku],
        ttl=timedelta(days=3650),
        schema=[Field(name="approved_returns_24h", dtype=Int64)],
        source=push,
    )
    store.apply([sku, push, view])
    monkeypatch.setattr(job, "_STORE", store)
    end = lambda h: int((T0 + timedelta(hours=h)).timestamp() * 1000)  # noqa: E731
    job.write_online(job.to_rows([("S1", end(1), 2), ("S1", end(2), 3), ("S2", end(1), 7)]))
    got = store.get_online_features(
        features=["sku_return_24h:approved_returns_24h"],
        entity_rows=[{"seller_sku": "S1"}, {"seller_sku": "S2"}, {"seller_sku": "S3"}],
    ).to_dict()
    assert got["approved_returns_24h"] == [3, 7, None]


@pytest.mark.parametrize("argv", [["--sink", "offline"], ["--sink", "both"]])
def test_cli_rejects_missing_offline_path_or_bad_sink(argv, monkeypatch):
    """Offline without a directory, or an unknown sink ⇒ stop immediately, without submitting the job."""
    monkeypatch.delenv("FEAST_SKU_RETURN_24H_PATH", raising=False)
    with pytest.raises(SystemExit):
        job.main(argv)
