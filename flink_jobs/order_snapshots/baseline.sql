-- Flink baseline: read the published Amazon topic, keeping the exact IDs and event time.
-- Runs continuously (not bounded), so the job stays RUNNING; CSV files close every ~30 s.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'flink-baseline-amazon';
SET 'execution.checkpointing.interval' = '10s';
-- Checkpoints to the EBS disk (the default keeps them in JobManager memory, max 5 MB).
SET 'state.checkpoints.dir' = 'file:///opt/flink/output/checkpoints';

CREATE TABLE amazon_notifications (
  EventTime STRING,
  NotificationMetadata ROW<NotificationId STRING>,
  Payload ROW<OrderChangeNotification ROW<AmazonOrderId STRING>>
) WITH (
  'connector' = 'kafka',
  'topic' = 'amazon.order-notifications',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'platform-flink-baseline-amazon',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'false'
);

CREATE TABLE baseline_amazon (
  notification_id STRING,
  order_id STRING,
  event_time STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/output/baseline-amazon-live',
  'format' = 'csv',
  'sink.rolling-policy.rollover-interval' = '30 s',
  'sink.rolling-policy.check-interval' = '10 s'
);

INSERT INTO baseline_amazon
SELECT NotificationMetadata.NotificationId,
       Payload.OrderChangeNotification.AmazonOrderId,
       EventTime
FROM amazon_notifications;
