-- Warehouse v2, migration 002: the PK already exists from 001 and moves with staging on rename.
-- Q1: 30-day created_at range; INCLUDE for the headline by day/channel/currency.
CREATE INDEX lifecycle_fact_sales ON warehouse_lifecycle.fact_order_lines (created_at) INCLUDE (channel, currency, headline_product_net_minor);
-- Q2: select the SKU before joining labels by dataset/line/generation.
CREATE INDEX lifecycle_fact_sku ON warehouse_lifecycle.fact_order_lines (seller_sku, dataset_version, line_id) INCLUDE (generation_id);
-- Q2: only labeled labels at the exact cutoff; the PK protects the grain for the remaining states.
CREATE INDEX lifecycle_label_cutoff ON warehouse_lifecycle.label_return (cutoff, dataset_version, line_id, generation_id) INCLUDE (label) WHERE state = 'labeled';
-- Q3: PIT lookup <= event_timestamp; keep created to pick a revision without looking into the future.
CREATE INDEX lifecycle_feature_pit ON warehouse_lifecycle.feat_sku_return_rate_90d (dataset_version, seller_sku, event_timestamp DESC, created DESC);
