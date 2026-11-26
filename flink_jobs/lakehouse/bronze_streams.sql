-- Streams into the lakehouse Bronze layer, always on (diagram: "Flink · stream to Bronze"). One job, three sinks:
--   amazon.order-notifications  → bronze.amazon_order_events   (dedup by NotificationId)
--   tiktok_shop.order-webhooks  → bronze.tiktok_order_events   (dedup by tts_notification_id: TikTok retries)
--   platform_cdc.walmart.orders → bronze.@@CDC_TABLE@@         (every Debezium change event, once)
-- Exactly-once: Kafka offsets and Iceberg commits both ride on the checkpoint (every minute); a redeploy resumes
-- from a savepoint (scripts/data/run_bronze_stream_jobs.py), so no offset is read twice or skipped.
-- @@…@@ placeholders are filled by that script (catalog, source epoch, run id).
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'lakehouse-bronze-streams';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.checkpoints.dir' = 'file:///opt/flink/output/checkpoints';
-- Dedup state: TikTok resends a webhook after 2 or 30 minutes, so one day of notification ids is plenty.
-- ponytail: a duplicate arriving > 1 day later lands in Bronze; Silver keeps the latest event per order anyway.
SET 'table.exec.state.ttl' = '1 d';
@@SAVEPOINT@@

-- ---------------------------------------------------------------- Kafka sources (raw value, kept as-is in Bronze)
CREATE TEMPORARY TABLE amazon_raw (
  payload STRING,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  proc AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'amazon.order-notifications',
  'properties.bootstrap.servers' = '@@KAFKA@@',
  'properties.group.id' = 'platform-flink-bronze-amazon',
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'raw'
);

CREATE TEMPORARY TABLE tiktok_raw (
  payload STRING,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  proc AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'tiktok_shop.order-webhooks',
  'properties.bootstrap.servers' = '@@KAFKA@@',
  'properties.group.id' = 'platform-flink-bronze-tiktok',
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'raw'
);

CREATE TEMPORARY TABLE walmart_cdc_raw (
  payload STRING,
  topic STRING METADATA FROM 'topic' VIRTUAL,
  kafka_partition INT METADATA FROM 'partition' VIRTUAL,
  kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = '@@CDC_TOPIC@@',
  'properties.bootstrap.servers' = '@@KAFKA@@',
  'properties.group.id' = 'platform-flink-bronze-walmart-cdc',
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'raw'
);

-- ---------------------------------------------------------------- Iceberg (Lakekeeper REST catalog)
CREATE CATALOG lakehouse WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = '@@CATALOG_URI@@',
  'warehouse' = '@@WAREHOUSE@@',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'@@S3_OPTIONS@@
);
CREATE DATABASE IF NOT EXISTS lakehouse.bronze;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.amazon_order_events (
  notification_id STRING,
  order_id STRING,
  channel_status STRING,
  event_time TIMESTAMP_LTZ(6),
  payload STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  ingested_at TIMESTAMP_LTZ(6)
);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.tiktok_order_events (
  notification_id STRING,
  order_id STRING,
  channel_status STRING,
  event_time TIMESTAMP_LTZ(6),
  payload STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  ingested_at TIMESTAMP_LTZ(6)
);

-- Same columns as the table the old Spark dp1_cdc job wrote, so its history stays in one table.
CREATE TABLE IF NOT EXISTS lakehouse.bronze.@@CDC_TABLE@@ (
  topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP_LTZ(6),
  event_json STRING,
  operation STRING,
  purchase_order_id STRING,
  source_lsn BIGINT,
  source_epoch STRING,
  ingested_at TIMESTAMP_LTZ(6),
  run_id STRING
);

EXECUTE STATEMENT SET
BEGIN

INSERT INTO lakehouse.bronze.amazon_order_events
SELECT notification_id, order_id, channel_status, event_time, payload, kafka_partition, kafka_offset, CURRENT_TIMESTAMP
FROM (
  SELECT JSON_VALUE(payload, '$.NotificationMetadata.NotificationId') AS notification_id,
         JSON_VALUE(payload, '$.Payload.OrderChangeNotification.AmazonOrderId') AS order_id,
         JSON_VALUE(payload, '$.Payload.OrderChangeNotification.Summary.OrderStatus') AS channel_status,
         CAST(TO_TIMESTAMP(JSON_VALUE(payload, '$.EventTime'), 'yyyy-MM-dd''T''HH:mm:ss''Z''') AS TIMESTAMP_LTZ(6)) AS event_time,
         payload, kafka_partition, kafka_offset,
         ROW_NUMBER() OVER (
           PARTITION BY JSON_VALUE(payload, '$.NotificationMetadata.NotificationId') ORDER BY proc ASC
         ) AS row_num
  FROM amazon_raw
  WHERE payload IS NOT NULL
)
WHERE row_num = 1;

INSERT INTO lakehouse.bronze.tiktok_order_events
SELECT notification_id, order_id, channel_status, event_time, payload, kafka_partition, kafka_offset, CURRENT_TIMESTAMP
FROM (
  SELECT JSON_VALUE(payload, '$.tts_notification_id') AS notification_id,
         JSON_VALUE(payload, '$.data.order_id') AS order_id,
         JSON_VALUE(payload, '$.data.order_status') AS channel_status,
         TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(payload, '$.timestamp') AS BIGINT), 0) AS event_time,
         payload, kafka_partition, kafka_offset,
         ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(payload, '$.tts_notification_id') ORDER BY proc ASC) AS row_num
  FROM tiktok_raw
  WHERE payload IS NOT NULL
)
WHERE row_num = 1;

-- Debezium tombstones (null value) carry no change: dropped, as the Spark job did.
INSERT INTO lakehouse.bronze.@@CDC_TABLE@@
SELECT topic, kafka_partition, kafka_offset, kafka_timestamp, payload,
       JSON_VALUE(payload, '$.op'),
       COALESCE(JSON_VALUE(payload, '$.after.purchase_order_id'), JSON_VALUE(payload, '$.before.purchase_order_id')),
       CAST(JSON_VALUE(payload, '$.source.lsn') AS BIGINT),
       '@@SOURCE_EPOCH@@', CURRENT_TIMESTAMP, '@@RUN_ID@@'
FROM walmart_cdc_raw
WHERE payload IS NOT NULL;

END;
