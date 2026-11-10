"""PyFlink stream feature: count of "approved" Returns per SKU over the last 24h (HOP 24h sliding 1h).

One file submits TWO jobs (DataStream API, Flink 2.2), differing in the sink:
  --sink online   Feast push ONLINE (Redis)            job ml-sku-return-24h-online
  --sink offline  Parquet append to an S3 directory (offline) job ml-sku-return-24h-offline
Each job reads Kafka with its own consumer group and recomputes the same count itself.

Event time = Kafka record timestamp (replay_return_events.py sets it to effective_at): watermarks are generated AT THE SOURCE, per
partition. Generating them after merging partitions lets a fast partition push the watermark past a slow one ⇒ messages are treated as late and
silently dropped (rehearsal with 401,129 messages on 3 partitions, 09/11: 37,284 wrong windows). PyFlink from_source ignores a Python TimestampAssigner.

Submit: scripts/run/run_stream_feature_jobs.sh (runs both). Replaces sku_return_feature_a.sql + feast-pusher.
"""

import argparse
import json
import os
from datetime import UTC, datetime

TOPIC = "order_snapshots.return_events"


def parse_event(raw):
    """JSON message → (record_sha256, seller_sku, event_ms) if it is a valid approved Return, otherwise None."""
    try:
        event = json.loads(raw)
        if event.get("event_type") != "approved" or int(event.get("quantity") or 0) <= 0:
            return None
        # Same as the SQL version: first 23 characters (to the millisecond), treated as UTC.
        at = datetime.fromisoformat(event["effective_at"][:23].replace("Z", "+00:00"))
        at = at if at.tzinfo else at.replace(tzinfo=UTC)
        return event["record_sha256"], event["seller_sku"], int(at.timestamp() * 1000)
    except (ValueError, KeyError, TypeError):
        return None


def to_rows(windows):
    """(sku, window_end_ms, count) → rows matching the sku_return_24h feature view schema."""
    return [
        {
            "seller_sku": sku,
            "event_timestamp": datetime.fromtimestamp(end_ms / 1000, tz=UTC),
            "approved_returns_24h": int(count),
        }
        for sku, end_ms, count in windows
    ]


def build_windows(raw_stream, assign_event_time=False):
    """JSON string stream → (sku, window_end_ms, count) for each HOP 24h/1h window with messages.

    The real job takes event time and watermarks from the Kafka source; `assign_event_time` is only for tests with a source that has no timestamps.
    """
    from pyflink.common import Duration, Time, Types, WatermarkStrategy
    from pyflink.common.watermark_strategy import TimestampAssigner
    from pyflink.datastream import KeyedProcessFunction
    from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
    from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
    from pyflink.datastream.window import SlidingEventTimeWindows

    event_type = Types.TUPLE([Types.STRING(), Types.STRING(), Types.LONG()])

    class EventTime(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return value[2]

    class DropDuplicates(KeyedProcessFunction):
        """Replaying the same Order Line twice ⇒ keep only the first copy (key = record_sha256)."""

        def open(self, runtime_context):
            seen = ValueStateDescriptor("seen", Types.BOOLEAN())
            # ponytail: a 7-day TTL bounds the state; a duplicate arriving after 7 days is counted again.
            seen.enable_time_to_live(StateTtlConfig.new_builder(Time.days(7)).build())
            self.seen = runtime_context.get_state(seen)
            self.duplicates = runtime_context.get_metrics_group().counter("duplicates_dropped")

        def process_element(self, value, ctx):
            if self.seen.value():
                self.duplicates.inc()
                return
            self.seen.update(True)
            yield value

    class Count(AggregateFunction):
        def create_accumulator(self):
            return 0

        def add(self, value, accumulator):
            return accumulator + 1

        def get_result(self, accumulator):
            return accumulator

        def merge(self, a, b):
            return a + b

    class WithWindowEnd(ProcessWindowFunction):
        def process(self, key, context, counts):
            yield key, context.window().end, next(iter(counts))

    deduped = (
        raw_stream.map(parse_event, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda e: e is not None)
        .map(lambda e: e, output_type=event_type)
        .key_by(lambda e: e[0], key_type=Types.STRING())
        .process(DropDuplicates(), output_type=event_type)
        .name("drop-duplicates")
    )
    if assign_event_time:
        deduped = deduped.assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(
                Duration.of_hours(1)
            ).with_timestamp_assigner(EventTime())
        )
    return (
        deduped.key_by(lambda e: e[1], key_type=Types.STRING())
        .window(SlidingEventTimeWindows.of(Time.hours(24), Time.hours(1)))
        .aggregate(
            Count(),
            WithWindowEnd(),
            accumulator_type=Types.LONG(),
            output_type=Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG()]),
        )
        .name("hop-24h-count")
    )


def write_online(rows):
    """Feast push ONLINE: Redis in exactly the Feast format the Feature API reads."""
    import pandas as pd
    from feast.data_source import PushMode

    store = _store()
    store.push("sku_return_24h_push", pd.DataFrame(rows), to=PushMode.ONLINE)


def write_offline(rows, base):
    """Write a new Parquet file into the offline directory (the Feast FileSource reads the whole directory)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                ("seller_sku", pa.string()),
                ("event_timestamp", pa.timestamp("us", tz="UTC")),
                ("approved_returns_24h", pa.int64()),
            ]
        ),
    )
    end_ms = [int(r["event_timestamp"].timestamp() * 1000) for r in rows]
    # ponytail: at-least-once; recovering from a checkpoint may rewrite a batch (duplicate rows with the same values, PIT join unchanged).
    name = f"part-{min(end_ms)}-{max(end_ms)}-{len(rows)}.parquet"
    fs, path = _filesystem(base)
    fs.create_dir(
        path.rstrip("/"), recursive=True
    )  # a regular file system needs the directory to exist; S3 ignores this
    pq.write_table(table, f"{path.rstrip('/')}/{name}", filesystem=fs)


_STORE = None


def _store():
    """One FeatureStore per Python worker (loads the S3 registry once)."""
    global _STORE
    if _STORE is None:
        from feast import FeatureStore

        _STORE = FeatureStore(os.environ.get("FEAST_REPO_PATH", "/opt/feature_repo"))
    return _STORE


def _filesystem(uri):
    """Real S3 (IAM role) or MinIO (FEAST_S3_ENDPOINT_URL); plain paths for tests."""
    from pyarrow import fs

    endpoint = os.environ.get("FEAST_S3_ENDPOINT_URL")
    if uri.startswith("s3://") and endpoint:
        scheme, host = endpoint.split("://", 1)
        return fs.S3FileSystem(endpoint_override=host, scheme=scheme), uri.removeprefix("s3://")
    return fs.FileSystem.from_uri(uri)


def attach_sink(windows, sink, flush_seconds, offline_path):
    """Batch windows by processing time (batch state lives in the checkpoint), then write once per batch."""
    from pyflink.common import Time, Types
    from pyflink.datastream.functions import ProcessAllWindowFunction
    from pyflink.datastream.window import TumblingProcessingTimeWindows

    class WriteBatch(ProcessAllWindowFunction):
        def open(self, runtime_context):
            group = runtime_context.get_metrics_group()
            self.rows = group.counter(f"{sink}_rows_written")
            self.batches = group.counter(f"{sink}_batches_written")
            self.last_window_end = 0
            group.gauge(f"{sink}_last_window_end_ms", lambda: self.last_window_end)

        def process(self, context, elements):
            rows = to_rows(elements)
            if sink == "online":
                write_online(rows)
            else:
                write_offline(rows, offline_path)
            self.rows.inc(len(rows))
            self.batches.inc()
            last = max(r["event_timestamp"] for r in rows)
            self.last_window_end = int(last.timestamp() * 1000)
            yield f"{sink}: wrote {len(rows)} rows, last {last.isoformat()}"

    return (
        windows.window_all(TumblingProcessingTimeWindows.of(Time.seconds(flush_seconds)))
        .process(WriteBatch(), output_type=Types.STRING())
        .name("feast-online-writer" if sink == "online" else "parquet-offline-writer")
        .print()
        .name("log")
    )


def main(argv=None):
    """Parse flags, build the job with the right sink, then run continuously."""
    from pyflink.common import Duration, WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sink", choices=["online", "offline"], required=True)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--offline-path", default=os.environ.get("FEAST_SKU_RETURN_24H_PATH"))
    # Online pushes fast (Redis); offline uses larger batches to avoid small files on S3.
    ap.add_argument("--flush-seconds", type=int)
    args = ap.parse_args(argv)
    if args.sink == "offline" and not args.offline_path:
        ap.error("--offline-path (or FEAST_SKU_RETURN_24H_PATH) is required with --sink offline")
    flush = args.flush_seconds or (2 if args.sink == "online" else 60)
    name = f"ml-sku-return-24h-{args.sink}"

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(
        10_000
    )  # checkpoint directory: the cluster's execution.checkpointing.dir
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap)
        .set_topics(args.topic)
        .set_group_id(name)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    # Per-partition watermarks, event time = Kafka record timestamp; idleness: a partition with no messages does not hold the watermark.
    watermarks = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_hours(1)).with_idleness(
        Duration.of_seconds(10)
    )
    raw = env.from_source(source, watermarks, "return_events")
    attach_sink(build_windows(raw), args.sink, flush, args.offline_path)
    env.execute(name)


if __name__ == "__main__":
    main()
