"""Independent stdlib oracle: reads only the observed sources and producer receipts, never Flink."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

FEEDS = {
    "carrier.delivery_events",
    "seller_postsales.return_events",
    "seller_postsales.refund_events",
    "seller_wms.stock_operations",
}


def canonical(value):
    """The oracle's own encoding to verify producer receipt digests."""
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()


def _load_source_records(root, manifest):
    """Read every source row of the stream feeds and check file checksums; keyed by `record_sha256`."""
    records = {}
    for item in manifest["files"]:
        if item["feed"] not in FEEDS or item.get("role", "source") != "source":
            continue
        path = (root / item["path"]).resolve()
        if not path.is_relative_to(root) or any(
            p.startswith("_") for p in path.relative_to(root).parts
        ):
            raise ValueError("not observable source")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != item["sha256"]:
            raise ValueError("source checksum mismatch")
        for line in raw.splitlines():
            row = json.loads(line)
            # The current export has no per-record hash yet: use the SHA256 of the source row without LF.
            sha = row.setdefault("record_sha256", hashlib.sha256(line).hexdigest())
            if sha in records:
                raise ValueError("duplicate source record")
            records[sha] = row
    return records


def _window_metric(row):
    """Name of the daily window metric this event contributes to, or None."""
    payload = row["payload"]
    if (
        row["feed"] == "seller_postsales.return_events"
        and payload["event_type"] == "approved"
        and payload["quantity"] > 0
    ):
        return "approved_return_events"
    if (
        row["feed"] == "seller_postsales.refund_events"
        and payload["status"] == "success"
        and payload["component"] == "product"
        and payload["amount_minor"] > 0
    ):
        return "successful_product_refund_events"
    return None


def _event_time(row):
    """Event time, truncated to milliseconds like Flink.

    effective_at is the specific lifecycle time: requested/approved/received,
    refund failed/success, stock operation; delivery uses actual_delivered_at.
    """
    ts = datetime.fromisoformat(
        row["payload"].get("actual_delivered_at", row["effective_at"]).replace("Z", "+00:00")
    )
    return ts.replace(microsecond=(ts.microsecond // 1000) * 1000)


def _check_manifest_counts(stream, groups, source_counts, duplicates):
    """Record counts and offsets per topic/partition must match the producer manifest; return totals per topic."""
    totals = {
        t: sum(v["input"] for (topic, _), v in groups.items() if topic == t)
        for t in stream["topics"]
    }
    for topic, values in stream["topics"].items():
        if (values["records"], values["source_records"], values["injected_duplicates"]) != (
            totals[topic],
            source_counts[topic],
            duplicates[topic],
        ):
            raise ValueError("manifest count mismatch")
        partitions = {
            str(p): dict(
                first_offset=v["first_offset"], last_offset=v["last_offset"], records=v["input"]
            )
            for (t, p), v in groups.items()
            if t == topic
        }
        if partitions != values["partitions"]:
            raise ValueError("manifest offset mismatch")
    return totals


def _expected_dedup(records, totals, prefix, epoch):
    """After dedup: each source record remains exactly once; other revisions of the same entity are kept."""
    dedup = []
    for topic in totals:
        rows = [r for r in records.values() if prefix + r["feed"] == topic]
        entities = {r["entity_id"] for r in rows}
        dedup.append(
            dict(
                topic=topic,
                epoch=epoch,
                input=totals[topic],
                dup_removed=totals[topic] - len(rows),
                kept=len(rows),
                revisions_kept=len(rows) - len(entities),
            )
        )
    return dedup


def expected(source, stream_manifest, delay_seconds=3600):
    """Compute baseline, simulated late, dedup and daily windows from the sources and the offset ledger.

    Replay the Kafka ledger in send order: each message updates the offset stats,
    the watermark (previous max event time − delay, per topic/partition) to classify main/late,
    and — the first time a record is seen — counts into the daily window.
    """
    if delay_seconds < 0:
        raise ValueError("negative watermark delay")
    root = Path(source).resolve()
    raw_manifest = (root / "manifest.json").read_bytes()
    manifest = json.loads(raw_manifest)
    stream = json.loads(Path(stream_manifest).read_text())
    if hashlib.sha256(raw_manifest).hexdigest() != stream["source_manifest_sha256"]:
        raise ValueError("source manifest mismatch")
    if manifest["dataset_version"] != stream["dataset_version"]:
        raise ValueError("dataset mismatch")
    prefix = stream.get("topic_prefix", "order_lifecycle.")
    if prefix not in ("order_lifecycle.", "order_lifecycle.late."):
        raise ValueError("invalid topic prefix")
    records = _load_source_records(root, manifest)
    groups, seen, windows = {}, set(), Counter()
    high, late = {}, Counter()
    digest, observed_sources, identities = hashlib.sha256(), set(), set()
    ordered = []
    previous_known = None
    duplicates = Counter()
    source_counts = Counter()
    for entry in stream["ledger"]:
        sha, topic, partition = entry["record_sha256"], entry["topic"], entry["partition"]
        row = records[sha]
        if topic != prefix + row["feed"]:
            raise ValueError("ledger topic mismatch")
        known = datetime.fromisoformat(row["source_observed_at"].replace("Z", "+00:00"))
        if previous_known is not None and known < previous_known:
            raise ValueError("ledger known-time order mismatch")
        previous_known = known
        if entry["retry"] != (sha in observed_sources):
            raise ValueError("retry identity mismatch")
        duplicates[topic] += entry["retry"]
        source_counts[topic] += not entry["retry"]
        observed_sources.add(sha)
        digest.update(canonical(row) + b"\n")
        key = (topic, partition)
        offset_key = (topic, partition, entry["offset"])
        if offset_key in identities:
            raise ValueError("duplicate Kafka offset")
        identities.add(offset_key)
        stats = groups.setdefault(
            key,
            dict(
                topic=topic,
                epoch=stream["epoch"],
                partition=partition,
                input=0,
                first_offset=entry["offset"],
                last_offset=entry["offset"],
            ),
        )
        stats["input"] += 1
        stats["first_offset"] = min(stats["first_offset"], entry["offset"])
        stats["last_offset"] = max(stats["last_offset"], entry["offset"])
        ts = _event_time(row)
        watermark = high.get(key)
        is_late = watermark is not None and ts <= watermark - timedelta(seconds=delay_seconds)
        if is_late:
            late[topic] += 1
        high[key] = max(ts, high.get(key, ts))
        ordered.append(
            dict(
                topic=topic,
                epoch=stream["epoch"],
                partition=partition,
                kafka_offset=entry["offset"],
                record_sha256=sha,
                entity_id=row["entity_id"],
                entity_revision=row["entity_revision"],
                route="late" if is_late else "main",
            )
        )
        if sha in seen:
            continue
        seen.add(sha)
        metric = _window_metric(row)
        if metric:
            windows[(topic, ts.date().isoformat(), metric)] += 1
    if seen != set(records) or digest.hexdigest() != stream["sha256"]:
        raise ValueError("ledger source coverage/digest mismatch")
    totals = _check_manifest_counts(stream, groups, source_counts, duplicates)
    return dict(
        epoch=stream["epoch"],
        delay_seconds=delay_seconds,
        topic_prefix=prefix,
        watermark_assumption="previous max(event_ts)-delay per topic/partition; millisecond precision; exact only for one partition per topic with on-event watermark emission",
        baseline=list(groups.values()),
        records=ordered,
        dedup=_expected_dedup(records, totals, prefix, stream["epoch"]),
        late=[
            dict(topic=t, epoch=stream["epoch"], input=n, main=n - late[t], late=late[t])
            for t, n in totals.items()
        ],
        windows=[
            dict(topic=t, epoch=stream["epoch"], day=d, metric=m, events=n)
            for (t, d, m), n in sorted(windows.items())
        ],
    )


def main():
    """Write the oracle separately; never accept Flink output arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--delay-seconds", type=int, default=3600)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    with Path(args.output).open("x") as handle:
        json.dump(
            expected(args.source, args.manifest, args.delay_seconds),
            handle,
            ensure_ascii=False,
            indent=2,
        )
        handle.write("\n")


if __name__ == "__main__":
    main()
