"""Render v2 SQL from receipt offsets: isolate the epoch before the watermark, check headers in the output."""

import argparse
import json
import re
from pathlib import Path

FEEDS = (
    "carrier.delivery_events",
    "seller_postsales.return_events",
    "seller_postsales.refund_events",
    "seller_wms.stock_operations",
)


def offset_ranges(manifest, epoch, prefix):
    """Check partitions/offsets are contiguous and match the ledger; turn the inclusive stop into exclusive."""
    if type(manifest.get("epoch")) is not int or manifest["epoch"] != epoch:
        raise ValueError("manifest epoch mismatch")
    if set(manifest["topics"]) != {prefix + feed for feed in FEEDS}:
        raise ValueError("manifest topics mismatch")
    for entry in manifest["ledger"]:
        if any(type(entry.get(k)) is not int or entry[k] < 0 for k in ("partition", "offset")):
            raise ValueError("invalid ledger partition/offset")
    result = {}
    for topic, stats in manifest["topics"].items():
        partitions = stats["partitions"]
        if not partitions:
            raise ValueError("empty topic has no explicit offset range")
        starts, stops = [], []
        count = 0
        if prefix == "order_lifecycle.late." and set(partitions) != {"0"}:
            raise ValueError("late proof requires partition 0 only")
        for partition, values in sorted(partitions.items(), key=lambda x: int(x[0])):
            if not re.fullmatch(r"0|[1-9][0-9]*", partition):
                raise ValueError("invalid partition")
            first, last, n = (values[k] for k in ("first_offset", "last_offset", "records"))
            if (
                any(type(x) is not int for x in (first, last, n))
                or first < 0
                or last < first
                or n != last - first + 1
            ):
                raise ValueError("non-contiguous epoch offsets")
            offsets = [
                r["offset"]
                for r in manifest["ledger"]
                if r["topic"] == topic and r["partition"] == int(partition)
            ]
            if offsets != list(range(first, last + 1)):
                raise ValueError("offset range does not match ordered ledger")
            count += n
            starts.append(f"partition:{partition},offset:{first}")
            stops.append(f"partition:{partition},offset:{last + 1}")
        if count != stats["records"]:
            raise ValueError("offset count mismatch")
        result[topic] = (";".join(starts), ";".join(stops))
    if sum(s["records"] for s in manifest["topics"].values()) != len(manifest["ledger"]):
        raise ValueError("unaccounted ledger records")
    return result


def expand_sources(sql, topics, ranges, proof):
    """Create one table per topic because Kafka specific-offsets only carry partition and offset."""
    pattern = r"CREATE TABLE (lifecycle_source|lifecycle_count_source) \(.*?\n\);"

    def expand(match):
        """Replace one source with single-topic sources plus a UNION ALL view."""
        name = match[1]
        tables = []
        for index, topic in enumerate(topics):
            table = match[0].replace(
                "CREATE TABLE " + name, "CREATE TABLE " + name + "_" + str(index), 1
            )
            table = re.sub(r"'topic' = '[^']+'", "'topic' = '" + topic + "'", table)
            table = table.replace("@@START_OFFSETS@@", ranges[topic][0]).replace(
                "@@STOP_OFFSETS@@", ranges[topic][1]
            )
            if proof:
                # Flink 1.20 watermark pushdown emits after each event, avoiding periodic-interval error.
                table = table.replace(
                    "'format' = 'json'",
                    "'scan.watermark.emit.strategy' = 'on-event',\n  'format' = 'json'",
                )
            tables.append(table)
        union = " UNION ALL ".join(f"SELECT * FROM {name}_{i}" for i in range(len(topics)))
        return "\n".join(tables) + f"\nCREATE VIEW {name} AS {union};"

    return re.sub(pattern, expand, sql, flags=re.S)


def render(
    output,
    *,
    epoch,
    manifest,
    bootstrap="kafka:9092",
    delay_seconds=3600,
    checkpoint_ms=1000,
    watermark_ms=1,
    late_topics=False,
    restart_mode=False,
):
    """Check config/receipt before writing; the proof generates four jobs over 11 single topics, parallelism 1."""
    if any(type(x) is not int for x in (epoch, delay_seconds, watermark_ms, checkpoint_ms)):
        raise ValueError("configuration must be integer")
    if (
        min(epoch, delay_seconds) < 0
        or watermark_ms <= 0
        or checkpoint_ms <= 0
        or delay_seconds >= 100 * 3600
    ):
        raise ValueError("invalid configuration")
    if not re.fullmatch(r"[a-zA-Z0-9_.:,\-]+", bootstrap):
        raise ValueError("invalid bootstrap")
    if restart_mode and late_topics:
        raise ValueError("restart mode requires regular dedup topics")
    receipt = json.loads(Path(manifest).read_text())
    prefix = "order_lifecycle.late." if late_topics else "order_lifecycle."
    ranges = offset_ranges(receipt, epoch, prefix)
    rendered = {}
    for source in sorted(
        (Path(__file__).resolve().parents[2] / "flink_jobs/order_lifecycle").glob("*.sql")
    ):
        if late_topics and source.stem != "late_arrival":
            continue
        groups = [[prefix + f] for f in FEEDS] if late_topics else [list(ranges)]
        for topics in groups:
            name = source.stem + ("_" + topics[0][len(prefix) :] if late_topics else "")
            sql = expand_sources(source.read_text(), topics, ranges, late_topics)
            if late_topics:
                sql = sql.replace("/order_lifecycle/late/", "/order_lifecycle/late-proof/")
            # This INTERVAL syntax is known to work on Flink 1.20.
            hours, rest = divmod(delay_seconds, 3600)
            delay_interval = f"INTERVAL '{hours}:{rest // 60:02d}:{rest % 60:02d}' HOUR TO SECOND"
            for key, value in dict(
                EPOCH=epoch,
                BOOTSTRAP=bootstrap,
                DELAY_INTERVAL=delay_interval,
                CHECKPOINT_MS=checkpoint_ms,
                WATERMARK_MS=watermark_ms,
                JOB=name,
            ).items():
                sql = sql.replace("@@" + key + "@@", str(value))
            if "@@" in sql:
                raise ValueError("unresolved SQL placeholder")
            if restart_mode and source.stem == "dedup":
                # The bounded summary must run after cancel, not after the unbounded DML.
                streaming, summary = sql.split(
                    "-- Synchronous DML waits for the streaming part to finish;", 1
                )
                summary = summary[summary.index("SET 'execution.runtime-mode' = 'batch';") :]
                preamble = sql[: sql.index("CREATE TABLE")]
                rendered["dedup_counts.sql"] = (
                    preamble.replace(
                        "order-lifecycle-stream-dedup-", "order-lifecycle-stream-dedup_counts-"
                    )
                    + summary
                )
                sql = re.sub(r"^\s*'scan\.bounded\.[^\n]+\n", "", streaming, flags=re.M)
                sql = sql.replace(
                    "SET 'table.dml-sync' = 'true';",
                    "SET 'table.dml-sync' = 'false';\nSET 'execution.runtime-mode' = 'streaming';",
                )
                # The JSON sink closes part files by processing time, then commits at the next checkpoint.
                sql = sql.replace(
                    "'format'='json'",
                    "'format'='json', "
                    "'sink.rolling-policy.rollover-interval'='1 s', "
                    "'sink.rolling-policy.check-interval'='1 s'",
                )
            rendered[name + ".sql"] = sql
    root = Path(output)
    root.mkdir(parents=True, exist_ok=False)
    for name, sql in rendered.items():
        (root / name).write_text(sql)


def main():
    """The CLI requires the manifest for the right epoch; never reads earliest/latest from shared topics."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--epoch", required=True, type=int)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--late-topics", action="store_true")
    parser.add_argument(
        "--restart-mode",
        action="store_true",
        help="Unbounded dedup and a separate bounded summary after REST cancel",
    )
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--delay-seconds", type=int, default=3600)
    parser.add_argument("--checkpoint-ms", type=int, default=1000)
    parser.add_argument("--watermark-ms", type=int, default=1)
    render(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
