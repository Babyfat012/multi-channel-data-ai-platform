-- Replaced on AWS by flink_jobs/ml/sku_return_feature.py (2 PyFlink jobs, online/offline); kept for the 30–31/10 run.
-- Pipeline A: count of "approved" Returns per SKU over the last 24h (HOP sliding 1h) → Kafka ml.sku_return_24h → feast-pusher.
-- Unlike version B (sku_return_feature.sql): messages already carry seller_sku (replayed from Gold A), so no PostgreSQL lookup.
-- Submit: FEATURE_SQL=flink_jobs/ml/sku_return_feature_a.sql scripts/run/run_sku_return_feature.sh
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'ml-sku-return-24h';
SET 'execution.checkpointing.interval' = '10s';
-- Checkpoints to disk (mount /mnt/platform-data/flink on EC2): the default keeps them in JobManager memory, max 5 MB;
-- dedup state + HOP of stream A is ~72 MB ⇒ every checkpoint failed and the job kept RESTARTING (30/10).
SET 'state.checkpoints.dir' = 'file:///opt/flink/output/checkpoints';
SET 'table.exec.source.idle-timeout' = '10s';

CREATE TABLE return_events (
  record_sha256 STRING,
  effective_at STRING,
  event_type STRING,
  quantity BIGINT,
  seller_sku STRING,
  event_ts AS TO_TIMESTAMP(REPLACE(SUBSTRING(effective_at, 1, 23), 'T', ' ')),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '1' HOUR
) WITH (
  'connector' = 'kafka',
  'topic' = 'order_snapshots.return_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'ml-sku-return-24h-a',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE sku_return_24h (
  seller_sku STRING,
  event_timestamp TIMESTAMP(3),
  approved_returns_24h BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ml.sku_return_24h',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'raw',
  'key.fields' = 'seller_sku',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

-- Replaying the same Order Line twice ⇒ keep the first copy by record_sha256.
CREATE VIEW approved AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY record_sha256 ORDER BY event_ts) AS rn
  FROM return_events
  WHERE event_type = 'approved' AND quantity > 0
) WHERE rn = 1;

-- Limitation: HOP only emits windows WITH messages; once a SKU has no more Returns, Redis keeps the old count until a new message arrives.
INSERT INTO sku_return_24h
SELECT seller_sku, window_end, COUNT(*)
FROM TABLE(HOP(TABLE approved, DESCRIPTOR(event_ts), INTERVAL '1' HOUR, INTERVAL '24' HOUR))
GROUP BY seller_sku, window_start, window_end;
