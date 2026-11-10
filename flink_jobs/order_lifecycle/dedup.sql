-- Bounded v2 source; render placeholders with scripts/run/render_order_lifecycle_stream.py before running.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'order-lifecycle-stream-@@JOB@@-@@EPOCH@@';
SET 'table.dml-sync' = 'true';
SET 'execution.checkpointing.interval' = '@@CHECKPOINT_MS@@ms';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.dir' = 'file:///opt/flink/checkpoints';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'pipeline.auto-watermark-interval' = '@@WATERMARK_MS@@ms';
CREATE TABLE lifecycle_source (
  feed STRING, record_sha256 STRING, entity_id STRING, entity_revision BIGINT,
  effective_at STRING, source_observed_at STRING,
  payload ROW<actual_delivered_at STRING, event_type STRING, quantity BIGINT,
              status STRING, component STRING, amount_minor BIGINT>,
  topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_headers MAP<STRING, BYTES> METADATA FROM 'headers' VIRTUAL,
  proc AS PROCTIME(),
  event_ts AS TO_TIMESTAMP(REPLACE(SUBSTRING(COALESCE(payload.actual_delivered_at, effective_at), 1, 23), 'T', ' ')),
  WATERMARK FOR event_ts AS event_ts - @@DELAY_INTERVAL@@
) WITH (
  'connector' = 'kafka',
  'topic' = 'v2.carrier.delivery_events;v2.seller_postsales.return_events;v2.seller_postsales.refund_events;v2.seller_wms.stock_operations',
  'properties.bootstrap.servers' = '@@BOOTSTRAP@@',
  'properties.group.id' = 'order-lifecycle-stream-@@JOB@@-@@EPOCH@@',
  'scan.startup.mode' = 'specific-offsets',
  'scan.startup.specific-offsets' = '@@START_OFFSETS@@',
  'scan.bounded.mode' = 'specific-offsets',
  'scan.bounded.specific-offsets' = '@@STOP_OFFSETS@@',
  'format' = 'json', 'json.ignore-parse-errors' = 'false'
);
CREATE VIEW epoch_source AS
SELECT *, CAST(CAST(kafka_headers['epoch'] AS STRING) AS BIGINT) AS epoch
FROM lifecycle_source;
CREATE TABLE kept (topic STRING, epoch BIGINT, `partition` INT, kafka_offset BIGINT, record_sha256 STRING, entity_id STRING, entity_revision BIGINT)  WITH ('connector'='filesystem', 'path'='file:///opt/flink/output/order_lifecycle/dedup/@@EPOCH@@/kept', 'format'='json');
CREATE TABLE audit (topic STRING, epoch BIGINT, `partition` INT, kafka_offset BIGINT, record_sha256 STRING, entity_id STRING, entity_revision BIGINT)  WITH ('connector'='filesystem', 'path'='file:///opt/flink/output/order_lifecycle/dedup/@@EPOCH@@/input', 'format'='json');

-- SHA partitions within an epoch are already isolated by offset: retries are gone, entity revisions remain.
EXECUTE STATEMENT SET BEGIN
 INSERT INTO audit SELECT topic, epoch, kafka_partition, kafka_offset, record_sha256, entity_id, entity_revision FROM epoch_source;
 INSERT INTO kept SELECT topic, epoch, kafka_partition, kafka_offset, record_sha256, entity_id, entity_revision FROM (
   SELECT *, ROW_NUMBER() OVER (PARTITION BY record_sha256 ORDER BY proc ASC) AS rn
   FROM epoch_source
 ) WHERE rn = 1;
END;
-- Synchronous DML waits for the streaming part to finish; batch only computes the final receipt totals.
SET 'execution.runtime-mode' = 'batch';
CREATE TABLE lifecycle_count_source (
  feed STRING, record_sha256 STRING, entity_id STRING, entity_revision BIGINT,
  effective_at STRING, source_observed_at STRING,
  payload ROW<actual_delivered_at STRING, event_type STRING, quantity BIGINT,
              status STRING, component STRING, amount_minor BIGINT>,
  topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_headers MAP<STRING, BYTES> METADATA FROM 'headers' VIRTUAL,
  event_ts AS TO_TIMESTAMP(REPLACE(SUBSTRING(COALESCE(payload.actual_delivered_at, effective_at), 1, 23), 'T', ' ')),
  WATERMARK FOR event_ts AS event_ts - @@DELAY_INTERVAL@@
) WITH (
  'connector' = 'kafka',
  'topic' = 'v2.carrier.delivery_events;v2.seller_postsales.return_events;v2.seller_postsales.refund_events;v2.seller_wms.stock_operations',
  'properties.bootstrap.servers' = '@@BOOTSTRAP@@',
  'properties.group.id' = 'order-lifecycle-stream-@@JOB@@-@@EPOCH@@',
  'scan.startup.mode' = 'specific-offsets',
  'scan.startup.specific-offsets' = '@@START_OFFSETS@@',
  'scan.bounded.mode' = 'specific-offsets',
  'scan.bounded.specific-offsets' = '@@STOP_OFFSETS@@',
  'format' = 'json', 'json.ignore-parse-errors' = 'false'
);
CREATE VIEW epoch_count_source AS
SELECT *, CAST(CAST(kafka_headers['epoch'] AS STRING) AS BIGINT) AS epoch
FROM lifecycle_count_source;
CREATE TABLE dedup_counts (topic STRING, epoch BIGINT, input BIGINT, dup_removed BIGINT, kept BIGINT, revisions_kept BIGINT)  WITH ('connector'='filesystem', 'path'='file:///opt/flink/output/order_lifecycle/dedup/@@EPOCH@@/counts', 'format'='json');

INSERT INTO dedup_counts
SELECT topic, epoch, COUNT(*), COUNT(*) - COUNT(DISTINCT record_sha256),
 COUNT(DISTINCT record_sha256), COUNT(DISTINCT record_sha256) - COUNT(DISTINCT entity_id)
FROM epoch_count_source GROUP BY topic, epoch;
