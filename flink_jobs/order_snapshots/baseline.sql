-- Baseline Flink: đọc topic Amazon đã phát, giữ đúng ID và event time.
-- Chạy liên tục (không bounded) nên job ở trạng thái RUNNING; file CSV đóng mỗi ~30 s.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'flink-baseline-amazon';
SET 'execution.checkpointing.interval' = '10s';
-- Checkpoint ra đĩa EBS (mặc định giữ ở bộ nhớ JobManager, tối đa 5 MB).
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
