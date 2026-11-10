"""Independently reconcile the 2 PyFlink jobs against the source file (without Flink): S3 offline per window, Redis latest value.

Run inside the Flink ML container (has Feast + environment variables), also fine while the jobs are running: only written windows are checked.
docker cp scripts/evidence/stream_feature_reconcile.py platform-flink-ml-jobmanager-1:/tmp/r.py
docker exec -i -e HOME=/tmp platform-flink-ml-jobmanager-1 python -W ignore /tmp/r.py < ~/return_events.jsonl
"""

import json
import os
import sys
from collections import Counter
from datetime import UTC, datetime

HOUR_MS = 3_600_000
SHOW = ("BEA-01708", "ELE-01084", "APP-00042")


def main():
    """Recount the HOP 24h/1h windows from the source file, then compare with S3 and Redis."""
    sys.path[:0] = ["/opt/flink/jobs/ml", "/opt/feature_repo"]
    import pyarrow.parquet as pq
    import sku_return_feature as job
    from feast import FeatureStore
    from replay_return_events import event_ms, to_event

    seen, counts, total = set(), Counter(), 0
    for line in sys.stdin:
        if not line.strip():
            continue
        event = to_event(json.loads(line))
        total += 1
        if event["record_sha256"] in seen:
            continue
        seen.add(event["record_sha256"])
        first = (event_ms(event) // HOUR_MS + 1) * HOUR_MS
        for k in range(24):
            counts[(event["seller_sku"], first + k * HOUR_MS)] += 1

    fs, path = job._filesystem(os.environ["FEAST_SKU_RETURN_24H_PATH"])
    rows = pq.read_table(path.rstrip("/"), filesystem=fs).to_pylist()
    got = {
        (r["seller_sku"], int(r["event_timestamp"].timestamp() * 1000)): r["approved_returns_24h"]
        for r in rows
    }
    end = max(k[1] for k in got)
    expected = {k: v for k, v in counts.items() if k[1] <= end}
    wrong = sum(1 for k, v in expected.items() if got.get(k) != v)
    extra = sum(1 for k in got if k not in expected)
    closed = datetime.fromtimestamp(end / 1000, tz=UTC).isoformat()
    print(
        f"source={total} unique={len(seen)} offline_closed_through={closed} rows={len(rows)} "
        f"windows={len(got)} expected={len(expected)} wrong_or_missing={wrong} extra={extra}"
    )

    # The two jobs run independently, so online may be a few hours ahead of offline: accept any correct value from the offline mark onward.
    by_sku = {}
    for (sku, window_end), value in counts.items():
        by_sku.setdefault(sku, []).append((window_end, value))
    allowed, latest = {}, {}
    for sku, windows in by_sku.items():
        closed_windows = [w for w in windows if w[0] <= end]
        if closed_windows:
            latest[sku] = max(closed_windows)[1]
            allowed[sku] = {latest[sku]} | {v for e, v in windows if e > end}
    skus = sorted(allowed)
    store = FeatureStore(os.environ.get("FEAST_REPO_PATH", "/opt/feature_repo"))
    values = store.get_online_features(
        features=["sku_return_24h:approved_returns_24h"],
        entity_rows=[{"seller_sku": s} for s in skus],
    ).to_dict()["approved_returns_24h"]
    online = dict(zip(skus, values, strict=True))
    bad = [s for s in skus if online[s] not in allowed[s]]
    print(f"redis sku={len(skus)} wrong={len(bad)} sample={[(s, online[s]) for s in bad[:3]]}")
    print(
        "Sample SKUs (Redis, last offline window):",
        [(s, online.get(s), latest.get(s)) for s in SHOW],
    )


if __name__ == "__main__":
    main()
