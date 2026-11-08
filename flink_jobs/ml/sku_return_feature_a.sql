-- Đã thay trên AWS bằng flink_jobs/ml/sku_return_feature.py (2 job PyFlink online/offline); giữ cho lần chạy 30–31/10.
-- Pipeline A: số Return "approved" mỗi SKU trong 24h gần nhất (HOP trượt 1h) → Kafka ml.sku_return_24h → feast-pusher.
-- Khác bản B (sku_return_feature.sql): tin đã mang seller_sku (phát lại từ Gold A) nên không tra PostgreSQL.
-- Submit: FEATURE_SQL=flink_jobs/ml/sku_return_feature_a.sql scripts/run/run_sku_return_feature.sh
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'ml-sku-return-24h';
SET 'execution.checkpointing.interval' = '10s';
-- Checkpoint ra đĩa (mount /mnt/platform-data/flink trên EC2): mặc định giữ ở bộ nhớ JobManager, tối đa 5 MB;
-- state bỏ trùng + HOP của stream A ~72 MB ⇒ mọi checkpoint hỏng, job RESTARTING mãi (30/10).
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

-- Phát lại hai lần cùng Order Line ⇒ giữ bản đầu theo record_sha256.
CREATE VIEW approved AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY record_sha256 ORDER BY event_ts) AS rn
  FROM return_events
  WHERE event_type = 'approved' AND quantity > 0
) WHERE rn = 1;

-- Giới hạn: HOP chỉ phát cửa sổ CÓ tin; SKU hết Return thì Redis giữ số cũ tới khi có tin mới.
INSERT INTO sku_return_24h
SELECT seller_sku, window_end, COUNT(*)
FROM TABLE(HOP(TABLE approved, DESCRIPTOR(event_ts), INTERVAL '1' HOUR, INTERVAL '24' HOUR))
GROUP BY seller_sku, window_start, window_end;
