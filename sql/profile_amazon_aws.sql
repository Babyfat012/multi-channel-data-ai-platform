-- Profile Amazon data characteristics on AWS (Trino EC2, lakehouse catalog): schema evolution, skew, cardinality, duplicates.
-- Run: docker compose --profile query exec -T trino trino --output-format ALIGNED < sql/profile_amazon_aws.sql
-- 1. Amazon schema evolution: JSON payload keys before (day 98) and after (day 101) the API change
SELECT export_date, arbitrary(array_join(array_sort(map_keys(CAST(json_parse(payload) AS map(varchar, json)))), ', ')) AS payload_keys
FROM lakehouse.bronze.raw_amazon_orders_dp1
WHERE export_date IN (DATE '2026-04-08', DATE '2026-04-12') GROUP BY export_date ORDER BY export_date;
-- 2. The two schema shapes after Silver unifies them
SELECT schema_form, min(export_date) AS from_date, max(export_date) AS to_date, count(*) AS order_lines
FROM lakehouse.silver.stg_amazon_order_lines_dp2 GROUP BY schema_form ORDER BY from_date;
-- 3. SKU skew (Zipf 0.8): share of Order Lines from the top 1% best-selling SKUs (20/2000) and the top 10 SKUs
WITH s AS (SELECT seller_sku, count(*) AS n FROM lakehouse.silver.stg_amazon_order_lines_dp2 GROUP BY seller_sku),
r AS (SELECT seller_sku, n, row_number() OVER (ORDER BY n DESC) AS rk, sum(n) OVER () AS total FROM s)
SELECT count(*) AS skus, round(100.0 * sum(n) FILTER (WHERE rk <= 20) / max(total), 2) AS top1pct_share,
       round(100.0 * sum(n) FILTER (WHERE rk <= 200) / max(total), 2) AS top10pct_share,
       max(n) AS max_lines, min(n) AS min_lines FROM r;
-- 4. Skew by storefront (marketplace) and cardinality (unique / total)
SELECT marketplace_id, count(*) AS order_lines, round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM lakehouse.silver.stg_amazon_order_lines_dp2 GROUP BY marketplace_id ORDER BY order_lines DESC;
SELECT count(*) AS order_lines,
       approx_distinct(order_id) AS order_id_unique, round(1.0 * approx_distinct(order_id) / count(*), 4) AS order_id_ratio,
       approx_distinct(postal_code) AS postal_unique, round(1.0 * approx_distinct(postal_code) / count(*), 6) AS postal_ratio,
       approx_distinct(city) AS city_unique, approx_distinct(seller_sku) AS sku_unique
FROM lakehouse.silver.stg_amazon_order_lines_dp2;
-- 5. Duplicates before Silver dedups: Bronze vs Silver (Order) for Amazon
SELECT (SELECT count(*) FROM lakehouse.bronze.raw_amazon_orders_dp1) AS bronze_rows,
       (SELECT count(DISTINCT order_id) FROM lakehouse.silver.stg_amazon_order_lines_dp2) AS silver_orders;
