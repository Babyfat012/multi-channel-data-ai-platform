-- Trino (EC2, lakehouse catalog): approved Returns (label = 1) of pipeline A in the @@DAYS@@ days before the cutoff,
-- with seller_sku from Gold, ordered by the time the Return became known. JSON rows feed feature_repo/replay_return_events.py.
-- REPLAYED from the answer ledger: stream A (Order notifications) has no Return events.
SELECT f.seller_sku, to_iso8601(l.label_available_at) AS label_available_at, l.order_line_key
FROM lakehouse.gold.fact_return_labels_dp3 AS l
JOIN lakehouse.gold.fact_order_lines_dp2 AS f ON f.order_line_key = l.order_line_key
WHERE l.label = 1
  AND l.label_available_at >= TIMESTAMP '@@CUTOFF@@' - INTERVAL '@@DAYS@@' DAY
  AND l.label_available_at < TIMESTAMP '@@CUTOFF@@'
ORDER BY l.label_available_at;
