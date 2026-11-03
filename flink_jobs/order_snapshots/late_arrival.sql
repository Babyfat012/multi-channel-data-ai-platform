-- Amazon: watermark trễ 1 giờ. Ghi riêng tin mà event time đã ở sau watermark.
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'flink-late-amazon';
SET 'execution.checkpointing.interval' = '10s';
-- Checkpoint ra đĩa EBS (mặc định giữ ở bộ nhớ JobManager, tối đa 5 MB).
SET 'state.checkpoints.dir' = 'file:///opt/flink/output/checkpoints';
SET 'table.exec.source.idle-timeout' = '10s';
-- Phát watermark mỗi 1 ms để bắt tin muộn chính xác khi backlog được đọc nhanh.
SET 'pipeline.auto-watermark-interval' = '1ms';

CREATE TABLE amazon_notifications (
  EventTime STRING,
  NotificationMetadata ROW<NotificationId STRING, PublishTime STRING>,
  kafka_partition BIGINT METADATA FROM 'partition' VIRTUAL,
  event_ts AS TO_TIMESTAMP(EventTime, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '1' HOUR
) WITH (
  'connector' = 'kafka',
  'topic' = 'amazon.order-notifications',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'platform-flink-late-amazon',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE late_amazon (
  notification_id STRING,
  event_time STRING,
  publish_time STRING,
  watermark_time STRING,
  kafka_partition BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/flink/output/late-amazon-live',
  'format' = 'csv',
  'sink.rolling-policy.rollover-interval' = '30 s',
  'sink.rolling-policy.check-interval' = '10 s'
);

INSERT INTO late_amazon
SELECT NotificationMetadata.NotificationId,
       EventTime,
       NotificationMetadata.PublishTime,
       CAST(CURRENT_WATERMARK(event_ts) AS STRING),
       kafka_partition
FROM amazon_notifications
WHERE CURRENT_WATERMARK(event_ts) IS NOT NULL
  AND event_ts <= CURRENT_WATERMARK(event_ts);
