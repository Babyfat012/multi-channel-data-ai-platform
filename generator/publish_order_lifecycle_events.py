"""Publish the four v2 operational feeds by known-time, keeping the envelope and a receipt per offset."""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

FEEDS = (
    "carrier.delivery_events",
    "seller_postsales.return_events",
    "seller_postsales.refund_events",
    "seller_wms.stock_operations",
)


def encoded(value):
    """Stable JSON encoding to hash receipts and resend the exact bytes on retry."""
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()


def source_rows(root):
    """Read exactly the declared source files, check hashes and reject paths outside the source."""
    root = Path(root).resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    rows = []
    for item in manifest["files"]:
        if item["feed"] not in FEEDS or item.get("role", "source") != "source":
            continue
        path = (root / item["path"]).resolve()
        if not path.is_relative_to(root) or any(
            p.startswith("_") for p in path.relative_to(root).parts
        ):
            raise ValueError("not an observable source path")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != item["sha256"]:
            raise ValueError("source file sha256 mismatch")
        for line in raw.splitlines():
            row = json.loads(line)
            row.setdefault("record_sha256", hashlib.sha256(line).hexdigest())
            if row["feed"] != item["feed"] or row["dataset_version"] != manifest["dataset_version"]:
                raise ValueError("source identity mismatch")
            rows.append(row)
    rows.sort(
        key=lambda r: (
            datetime.fromisoformat(r["source_observed_at"].replace("Z", "+00:00")),
            r["feed"],
            r["source_sequence"],
            r["source_event_id"],
        )
    )
    hashes = [r["record_sha256"] for r in rows]
    if len(hashes) != len(set(hashes)):
        raise ValueError("source already contains retries; expected unique observed envelopes")
    return manifest, rows


class FileSink:
    """Write a global JSONL log, simulating partition 0 and per-topic increasing offsets."""

    def __init__(self, path):
        """Open a new target; never overwrite a previous publish."""
        self.handle = Path(path).open("x")  # noqa: SIM115 (closed by close())
        self.offsets = Counter()

    def send(self, topic, key, value, epoch, delivered):
        """Write the envelope as is, the epoch header and the offset to the test log."""
        offset = self.offsets[topic]
        self.offsets[topic] += 1
        self.handle.write(
            encoded(
                dict(
                    topic=topic,
                    key=key,
                    value=value,
                    headers={"epoch": str(epoch)},
                    partition=0,
                    offset=offset,
                )
            ).decode()
            + "\n"
        )
        delivered(0, offset)

    def close(self):
        """Close the file so all receipts are durably written."""
        self.handle.close()


class KafkaSink:
    """Use the installed confluent-kafka and collect offsets from the real delivery callback."""

    def __init__(self, bootstrap):
        """Enable idempotence so only intentional retries create extra records."""
        self.bootstrap = bootstrap
        from confluent_kafka import Producer

        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
        self.errors = []

    def send(self, topic, key, value, epoch, delivered):
        """Send the same envelope bytes with the epoch header; never add the epoch to the payload."""

        def callback(error, message):
            """Store the error or confirm the offset of a record the broker accepted."""
            if error:
                self.errors.append(str(error))
            else:
                delivered(message.partition(), message.offset())

        while True:
            try:
                self.producer.produce(
                    topic,
                    key=key.encode(),
                    value=encoded(value),
                    headers={"epoch": str(epoch).encode()},
                    on_delivery=callback,
                )
                break
            except BufferError:
                self.producer.poll(0.1)
        self.producer.poll(0)

    def close(self):
        """Only accept the manifest as complete if every record was confirmed."""
        remaining = self.producer.flush(120)
        if remaining or self.errors:
            raise RuntimeError(f"Kafka delivery incomplete: {remaining}, {self.errors[:3]}")


def run_stream(
    source,
    sink,
    manifest_path,
    *,
    epoch,
    duplicate_rate=0.03,
    seed=7,
    pause_ms=0,
    late_topics=False,
    topic_prefix="order_lifecycle.",
):
    """Publish N sources plus floor(N×rate) seed-chosen retries and write a ledger with real offsets."""
    if type(epoch) is not int or epoch < 0 or not 0 <= duplicate_rate <= 1 or pause_ms < 0:
        raise ValueError("invalid epoch/rate/pause")
    manifest_path = Path(manifest_path)
    if manifest_path.exists():
        raise FileExistsError(manifest_path)
    if topic_prefix == "order_lifecycle.late." and isinstance(sink, KafkaSink):
        for feed in FEEDS:
            metadata = sink.producer.list_topics(topic=topic_prefix + feed, timeout=10)
            topic = metadata.topics[topic_prefix + feed]
            if topic.error or set(topic.partitions) != {0}:
                raise ValueError("late topic must have exactly one partition: " + feed)
    source_manifest, rows = source_rows(source)
    selected = set(random.Random(seed).sample(range(len(rows)), int(len(rows) * duplicate_rate)))
    ledger, topics = (
        [],
        {
            f"{topic_prefix}{f}": {
                "source_records": 0,
                "records": 0,
                "injected_duplicates": 0,
                "partitions": {},
            }
            for f in FEEDS
        },
    )
    digest = hashlib.sha256()
    try:
        for index, row in enumerate(rows):
            topic = topic_prefix + row["feed"]
            topics[topic]["source_records"] += 1
            for attempt in range(2 if index in selected else 1):
                entry = dict(topic=topic, record_sha256=row["record_sha256"], retry=bool(attempt))
                ledger.append(entry)
                digest.update(encoded(row) + b"\n")
                topics[topic]["records"] += 1
                topics[topic]["injected_duplicates"] += bool(attempt)

                def delivered(partition, offset, entry=entry, topic=topic):
                    """Attach the real offset to the matching receipt row."""
                    entry.update(partition=partition, offset=offset)
                    stats = topics[topic]["partitions"].setdefault(
                        str(partition), dict(first_offset=offset, last_offset=offset, records=0)
                    )
                    stats["first_offset"] = min(stats["first_offset"], offset)
                    stats["last_offset"] = max(stats["last_offset"], offset)
                    stats["records"] += 1

                sink.send(topic, row["entity_id"], row, epoch, delivered)
                if pause_ms:
                    time.sleep(pause_ms / 1000)
    finally:
        sink.close()
    manifest = dict(
        topic_prefix=topic_prefix,
        version="stream-v2/1",
        epoch=epoch,
        dataset_version=source_manifest["dataset_version"],
        source_manifest_sha256=hashlib.sha256(
            (Path(source) / "manifest.json").read_bytes()
        ).hexdigest(),
        duplicate_rate=duplicate_rate,
        seed=seed,
        sha256=digest.hexdigest(),
        sha256_policy="canonical envelopes + LF, in send order, including retries",
        topics=topics,
        ledger=ledger,
    )
    if late_topics:
        manifest["late_manifest"] = str(manifest_path.with_name(manifest_path.stem + "-late.json"))
    with manifest_path.open("x") as handle:
        handle.write(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    if late_topics:

        def sink_factory():
            """Create a new sink for the proof copy; never reuse a closed sink."""
            if isinstance(sink, FileSink):
                return FileSink(str(sink.handle.name) + ".late")
            return KafkaSink(sink.bootstrap)

        # Publish an independent copy with the same order/retries to the pre-created single-partition topic.
        run_stream(
            source,
            sink_factory(),
            manifest["late_manifest"],
            epoch=epoch,
            duplicate_rate=duplicate_rate,
            seed=seed,
            pause_ms=pause_ms,
            topic_prefix="order_lifecycle.late.",
        )
    return manifest


def main():
    """CLI choosing file or Kafka; each epoch must use a new manifest and log."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--epoch", required=True, type=int)
    sink = parser.add_mutually_exclusive_group(required=True)
    sink.add_argument("--bootstrap")
    sink.add_argument("--file-out")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--duplicate-rate", type=float, default=0.03)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--late-topics",
        action="store_true",
        help="Also publish v2.late.<feed>; each topic must have 1 partition",
    )
    parser.add_argument("--pause-ms", type=float, default=0)
    args = parser.parse_args()
    run_stream(
        args.source,
        FileSink(args.file_out) if args.file_out else KafkaSink(args.bootstrap),
        args.manifest,
        epoch=args.epoch,
        duplicate_rate=args.duplicate_rate,
        seed=args.seed,
        pause_ms=args.pause_ms,
        late_topics=args.late_topics,
    )


if __name__ == "__main__":
    main()
