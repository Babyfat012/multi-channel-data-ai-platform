-- The same stream of UNPAID Orders, two windows to compare burst detection time.
-- TUMBLE 5 minutes updates every 5 minutes; HOP 5 minutes updates every 1 minute.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'flink-window-tiktok';
SET 'execution.checkpointing.interval' = '10s';
-- Checkpoints to the EBS disk (the default keeps them in JobManager memory, max 5 MB).
SET 'state.checkpoints.dir' = 'file:///opt/flink/output/checkpoints';
SET 'table.exec.source.idle-timeout' = '10s';

CREATE TABLE tiktok_webhooks (
  tts_notification_id STRING,
  `timestamp` BIGINT,
  data ROW<order_id STRING, order_status STRING>,
  event_ts AS TO_TIMESTAMP_LTZ(`timestamp`, 0),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'tiktok_shop.order-webhooks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'platform-flink-window-tiktok',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE VIEW new_orders AS
SELECT tts_notification_id, event_ts
FROM tiktok_webhooks
WHERE data.order_status = 'UNPAID';

CREATE TABLE tiktok_tumble_5m (
  window_start STRING,
  window_end STRING,
  orders BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/output/tiktok-tumble-5m-live',
  'format' = 'csv',
  'sink.rolling-policy.rollover-interval' = '30 s',
  'sink.rolling-policy.check-interval' = '10 s'
);

CREATE TABLE tiktok_hop_5m_1m (
  window_start STRING,
  window_end STRING,
  orders BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/output/tiktok-hop-5m-1m-live',
  'format' = 'csv',
  'sink.rolling-policy.rollover-interval' = '30 s',
  'sink.rolling-policy.check-interval' = '10 s'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO tiktok_tumble_5m
  SELECT CAST(window_start AS STRING), CAST(window_end AS STRING), COUNT(*)
  FROM TABLE(TUMBLE(TABLE new_orders, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE))
  GROUP BY window_start, window_end;

  INSERT INTO tiktok_hop_5m_1m
  SELECT CAST(window_start AS STRING), CAST(window_end AS STRING), COUNT(*)
  FROM TABLE(HOP(TABLE new_orders, DESCRIPTOR(event_ts),
                 INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))
  GROUP BY window_start, window_end;
END;
