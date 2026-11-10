-- ML Platform stream feature: count of "approved" Returns per SKU over the last 24h (HOP sliding 1h).
-- Kafka return_events → drop duplicate retries → look up line_id→SKU in PostgreSQL → Kafka ml.sku_return_24h.
-- Runs continuously (streaming). @@PG_*@@ is filled by scripts/run/run_sku_return_feature.sh at submit time.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'ml-sku-return-24h';
SET 'execution.checkpointing.interval' = '10s';
-- Checkpoints to disk (mount /mnt/platform-data/flink on EC2): the default keeps them in JobManager memory, max 5 MB;
-- dedup state + HOP of stream A is ~72 MB ⇒ every checkpoint failed and the job kept RESTARTING (30/10).
SET 'state.checkpoints.dir' = 'file:///opt/flink/output/checkpoints';
-- A Kafka partition with no messages does not hold the watermark back forever.
SET 'table.exec.source.idle-timeout' = '10s';

CREATE TABLE return_events (
  record_sha256 STRING,
  effective_at STRING,
  payload ROW<event_type STRING, quantity BIGINT, line_id STRING>,
  dataset_version STRING,
  event_ts AS TO_TIMESTAMP(REPLACE(SUBSTRING(effective_at, 1, 23), 'T', ' ')),
  proc_time AS PROCTIME(),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '1' HOUR
) WITH (
  'connector' = 'kafka',
  'topic' = 'order_lifecycle.seller_postsales.return_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'ml-sku-return-24h',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Lookup table published by the dp_order_lifecycle_warehouse DAG; cached so the DB is not called per message.
CREATE TABLE line_sku (
  dataset_version STRING, line_id STRING, seller_sku STRING,
  PRIMARY KEY (dataset_version, line_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/@@PG_DB@@',
  'table-name' = 'warehouse_lifecycle.fact_order_lines',
  'username' = '@@PG_USER@@',
  'password' = '@@PG_PASSWORD@@',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '100000',
  'lookup.partial-cache.expire-after-write' = '1h'
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

-- Producer retries replay the exact bytes ⇒ keep the first copy by record_sha256 (preserves rowtime).
CREATE VIEW approved AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY record_sha256 ORDER BY event_ts) AS rn
  FROM return_events
  WHERE payload.event_type = 'approved' AND payload.quantity > 0
) WHERE rn = 1;

CREATE VIEW approved_sku AS
SELECT a.event_ts, l.seller_sku
FROM approved AS a
JOIN line_sku FOR SYSTEM_TIME AS OF a.proc_time AS l
  ON a.dataset_version = l.dataset_version AND a.payload.line_id = l.line_id;

-- Limitation: HOP only emits windows WITH messages; once a SKU has no more Returns, Redis keeps the old count until a new message arrives.
-- To really return to 0, add a scheduled stream that emits 0.
INSERT INTO sku_return_24h
SELECT seller_sku, window_end, COUNT(*)
FROM TABLE(HOP(TABLE approved_sku, DESCRIPTOR(event_ts), INTERVAL '1' HOUR, INTERVAL '24' HOUR))
GROUP BY seller_sku, window_start, window_end;
