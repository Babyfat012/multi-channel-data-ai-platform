-- Live new-order counts for Grafana ("Live orders / 5 min"), always on, separate from the Bronze job so a Postgres
-- outage only restarts this one. Counts the first status event of each order (Amazon `Pending`, TikTok `UNPAID`)
-- per channel in 5-minute event-time windows → Postgres platform_ops.ops.order_counts_window.
-- COUNT(DISTINCT order_id): TikTok retries a webhook with the same timestamp, so a retry lands in the same window.
-- ponytail: events later than the 30 s watermark are dropped from the count; Bronze still has them.
-- Upsert on (channel, window_start): a restart from a checkpoint rewrites the same rows instead of duplicating them.
-- @@…@@ placeholders are filled by scripts/data/run_ops_order_counts.py.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'ops-order-counts';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '60s';
SET 'state.checkpoints.dir' = 'file:///opt/flink/output/checkpoints';
-- One quiet topic must not hold the watermark (and every window) back.
SET 'table.exec.source.idle-timeout' = '30s';
@@SAVEPOINT@@

CREATE TEMPORARY TABLE amazon_raw (
  payload STRING,
  event_ts AS CAST(TO_TIMESTAMP(JSON_VALUE(payload, '$.EventTime'), 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS TIMESTAMP_LTZ(3)),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'amazon.order-notifications',
  'properties.bootstrap.servers' = '@@KAFKA@@',
  'properties.group.id' = 'platform-flink-ops-amazon',
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'latest',
  'format' = 'raw'
);

CREATE TEMPORARY TABLE tiktok_raw (
  payload STRING,
  event_ts AS TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(payload, '$.timestamp') AS BIGINT), 0),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'tiktok_shop.order-webhooks',
  'properties.bootstrap.servers' = '@@KAFKA@@',
  'properties.group.id' = 'platform-flink-ops-tiktok',
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'latest',
  'format' = 'raw'
);

CREATE TEMPORARY TABLE order_counts_window (
  channel STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  orders BIGINT,
  PRIMARY KEY (channel, window_start) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = '@@JDBC_URL@@',
  'table-name' = 'ops.order_counts_window',
  'username' = '@@PG_USER@@',
  'password' = '@@PG_PASSWORD@@'
);

CREATE TEMPORARY VIEW new_orders AS
SELECT 'amazon' AS channel, JSON_VALUE(payload, '$.Payload.OrderChangeNotification.AmazonOrderId') AS order_id, event_ts
FROM amazon_raw
WHERE JSON_VALUE(payload, '$.Payload.OrderChangeNotification.Summary.OrderStatus') = 'Pending'
UNION ALL
SELECT 'tiktok_shop', JSON_VALUE(payload, '$.data.order_id'), event_ts
FROM tiktok_raw
WHERE JSON_VALUE(payload, '$.data.order_status') = 'UNPAID';

INSERT INTO order_counts_window
SELECT channel, CAST(window_start AS TIMESTAMP(3)), CAST(window_end AS TIMESTAMP(3)), COUNT(DISTINCT order_id)
FROM TABLE(TUMBLE(TABLE new_orders, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE))
GROUP BY channel, window_start, window_end;
