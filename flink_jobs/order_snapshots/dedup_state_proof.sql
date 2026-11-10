-- A separate run to capture the Flink UI and execution plan of the keyed dedup.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'dedup_tiktok_state_proof';

CREATE TABLE tiktok_webhooks (
  tts_notification_id STRING,
  `timestamp` BIGINT,
  data ROW<order_id STRING, order_status STRING>,
  proc AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'tiktok_shop.order-webhooks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'platform-flink-dedup-state-proof',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE TABLE unique_tiktok_webhooks_proof (
  notification_id STRING,
  order_id STRING,
  order_status STRING,
  event_time BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/output/unique-tiktok-state-proof',
  'format' = 'csv'
);

INSERT INTO unique_tiktok_webhooks_proof
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
