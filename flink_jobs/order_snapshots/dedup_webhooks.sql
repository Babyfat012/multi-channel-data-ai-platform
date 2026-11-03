-- TikTok Shop: giữ webhook đầu tiên theo tts_notification_id bằng Flink state.
-- ROW_NUMBER ... ORDER BY proc ASC là pattern dedup append-only của Flink SQL.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'flink-dedup-tiktok';
SET 'execution.checkpointing.interval' = '10s';
-- Checkpoint ra đĩa EBS (mặc định giữ ở bộ nhớ JobManager, tối đa 5 MB).
SET 'state.checkpoints.dir' = 'file:///opt/flink/output/checkpoints';

CREATE TABLE tiktok_webhooks (
  tts_notification_id STRING,
  `timestamp` BIGINT,
  data ROW<order_id STRING, order_status STRING>,
  proc AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'tiktok_shop.order-webhooks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'platform-flink-dedup-tiktok',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE unique_tiktok_webhooks (
  notification_id STRING,
  order_id STRING,
  order_status STRING,
  event_time BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/output/unique-tiktok-live',
  'format' = 'csv',
  'sink.rolling-policy.rollover-interval' = '30 s',
  'sink.rolling-policy.check-interval' = '10 s'
);

INSERT INTO unique_tiktok_webhooks
SELECT tts_notification_id, order_id, order_status, `timestamp`
FROM (
  SELECT tts_notification_id, data.order_id AS order_id,
         data.order_status AS order_status, `timestamp`,
         ROW_NUMBER() OVER (
           PARTITION BY tts_notification_id ORDER BY proc ASC
         ) AS row_num
  FROM tiktok_webhooks
)
WHERE row_num = 1;
