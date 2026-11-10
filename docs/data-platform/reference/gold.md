# Gold Order Lines and offline features

## Table `lakehouse.gold.fact_order_lines_dp2`

Grain: **one product in one Order of one Channel**. The job
`spark_jobs/gold/build_order_snapshot_lines.py` merges the five Silver tables after each table has
been validated. The key `order_line_key` is the SHA-256 of `channel`, the original Order ID,
the secondary Order ID if any, and the original Order Line ID. Walmart always has
`channel_secondary_order_id = customer_order_id`, so two Orders sharing a
`purchase_order_id` stay separate.

| Column group | Meaning |
|---|---|
| `channel`, `channel_order_id`, `channel_secondary_order_id`, `channel_line_id`, `order_line_key` | Identifiers and the key across the five Channels |
| `amazon_order_version_key` | Amazon SCD2 version key matching the snapshot in the fact; null for the other four Channels |
| `created_at`, `last_updated_time`, `order_status`, `channel_status` | Observed snapshot time and status |
| `seller_sku`, `product_name`, `quantity` | The Seller's product; names are available for Walmart and the three new Channels, Amazon may be null |
| `line_gross_amount`, `line_discount_amount`, `line_net_amount`, `currency` | Money per Order Line; Walmart has only the net amount, the gross/discount columns are null |
| `city`, `postal_code`, `country` | Shipping address from the Channel |
| `estimated_delivery_start`, `estimated_delivery_end` | **Estimated** timestamps; not the actual delivery date |
| `source_export_date`, `source_record_ref`, `source_silver_table`, `source_run_id`, `gold_created_at`, `run_id` | Source and processing-run lineage |

Money columns keep the source's currency unit. Total revenue queries must group by
`currency`; USD and VND are not summed without an exchange rate. The table has no return
label or actual delivery time. For the decisions and source facts see
`docs/data-platform/reference/gold_design.md`.

## Table `lakehouse.gold.dim_amazon_order_dp2`

One row per `(order_id, valid_from_ts)` pair observed in the Amazon Bronze
exports. `valid_from_ts` is the status's `LastUpdateDate`/`lastUpdatedTime`;
`valid_to_ts` is the start of the next version and excludes that timestamp.
The last version has `valid_to_ts = NULL`, `is_current = true`. The column
`first_observed_export_date` keeps the first date the system saw the version. The key
`spark_jobs.lib.order_version_key` is the SHA-256 of the Order ID and the status change time, computed
the same way as `amazon_order_version_key` in the fact. If two exports with the same timestamp
have different canonical statuses, the job stops; two source spellings such as `Shipped` and
`SHIPPED` are treated as the same status, keeping the source text from the latest export.

DP2 run `manual__2026-10-14T03:53:52+00:00`, after adding SCD2, completed
**14/14 tasks**. Validate recorded **5,779,561** versions, including **5,734,309** current
versions and **45,252** expired versions; `invalid=0`,
`duplicate=0`, `multiple_current=0`, `bad_fact=0`, `missing_version=0`.
The Gold fact also passed `invalid=0`, `duplicate_order_line_key=0` after the rewrite.

![Airflow Graph DP2 with SCD2 and Gold, 14 tasks success](../../images/airflow/dp2_silver-graph-scd2-five-channels.png)

![Airflow Grid DP2 with SCD2 and Gold](../../images/airflow/dp2_silver-grid-scd2-five-channels.png)

## Table `lakehouse.gold.feat_order_line_return_dp3`

Same grain as the fact. The job `spark_jobs/gold/build_order_snapshot_return_features.py` takes only columns that
can be known **at the moment the Order is placed**: Channel, SKU, money, quantity, country and
calendar components of `created_at`. `event_timestamp` equals `created_at`; `created`
is when the feature table was written. These two columns separate event time from the time
the feature was computed. Walmart has no original price or discount, so
`discount_fraction` is null for that Channel.

The job does not read `order_status`, `channel_status`, `last_updated_time`,
`delivered_at`, labels or `label_available_at` as features. Walmart labels are
joined in a separate table via `spark_jobs/gold/build_walmart_order_snapshot_labels.py` per
decision Q3, not in the feature table.

## Runtime confirmation

Before adding SCD2, the same DP2 run had 12/12 tasks and the Gold table was written from the five
Silver tables. The Order Line distribution did not change after adding the version key:

| Channel | Silver | Gold |
|---|---:|---:|
| Amazon | 8,314,312 | 8,314,312 |
| Walmart | 1,875,607 | 1,875,607 |
| TikTok Shop | 2,609 | 2,609 |
| Shopify | 1,316 | 1,316 |
| Shopee | 514 | 514 |
| **Total** | **10,194,358** | **10,194,358** |

The Validate log records `Gold invalid=0, duplicate_order_line_key=0`; the table has no
`label`, `delivered_at` or `label_available_at`. The Amazon source here is a
10-day window and the three new Channels use small sources, per the DP1 scope recorded in
[`airflow.md`](../pipeline_orchestration.md).

![Airflow Graph DP2 with Silver and Gold, 12 tasks success](../../images/airflow/dp2_silver-graph-gold-five-channels.png)

![Airflow Grid DP2 with Silver and Gold](../../images/airflow/dp2_silver-grid-gold-five-channels.png)

DP3 run `manual__2026-10-14T04:11:15+00:00` completed 2/2 tasks. The feature table
has **10,194,358** rows, equal to Gold, with the per-Channel distribution matching the table above.
`validate_return_features` recorded `feature invalid=0`,
`duplicate_order_line_key=0` and `PASS`. Validate also blocks status and time columns
that only exist after the Order is placed, to prevent future information leakage.

![Airflow Graph DP3 features, Build and Validate succeeded](../../images/airflow/dp3_features-graph-five-channels.png)

![Airflow Grid DP3 features](../../images/airflow/dp3_features-grid-five-channels.png)

After Gold added `amazon_order_version_key`, the same DP3 run was rerun and
completed **2/2 tasks** at 05:02:40 UTC. The second Validate again recorded
`feature invalid=0`, `duplicate_order_line_key=0`. The feature job only reads columns
known at Order placement, so the new status version key does not enter the features.

![Airflow Graph DP3 after SCD2](../../images/airflow/dp3_features-graph-scd2-five-channels.png)

![Airflow Grid DP3 after SCD2](../../images/airflow/dp3_features-grid-scd2-five-channels.png)

## Separate Walmart labels

`scripts/run/prepare_walmart_label_keys.py` reads the ground-truth mapping in batches,
using seed 42 from the manifest and the generator's `native_digits` function to add
`customer_order_id`. The prepared Parquet file has **15,562,675** rows; this step must be
rerun if the source or seed changes:

```bash
.venv/bin/python scripts/run/prepare_walmart_label_keys.py
```

DAG `dp3_walmart_labels`, run `manual__2026-10-14T04:15:18+00:00`, wrote the table
`lakehouse.gold.fact_walmart_return_labels_dp3` with **1,319,345** labeled Walmart
Order Lines. The task log records `duplicate=0`, `invalid=0`. This is a separate label table
for training and evaluation and must not be joined into the features at prediction
time. The mapping has 15.56 million rows for the whole ground truth, while Walmart Gold
in this run has only 1.88 million Order Lines; so having fewer labeled rows than the
mapping is consistent with the trial table's scope and is not a measurement of full-period coverage.

![Airflow Graph Walmart label bridge succeeded](../../images/airflow/dp3_walmart_labels-graph-five-channels.png)

![Airflow Grid Walmart label bridge](../../images/airflow/dp3_walmart_labels-grid-five-channels.png)

## On AWS

DP2/DP3 ran Spark on Compose EC2, with Iceberg tables on S3; the run_ids are in the
[AWS manifest](../../evidence/aws/datahub-validation-aws-20261029.json).
`fact_order_lines_dp2` unions the five Silver tables into 227,200,303 Order Lines;
`dim_amazon_order_dp2` keeps 106,109,637 current versions and 1,062,966 historical versions.
`feat_order_line_return_dp3` keeps the attributes at Order placement, also 227,200,303 rows.

![Session Manager EC2: DP2 log records Silver validate PASS, Gold matching per Channel and Amazon SCD2 without errors](../../evidence/aws/dp2-validate-logs-20261028.png)

![Session Manager EC2: fact_rows and feature_rows both 227,200,303, missing, extra and wrong_event_time equal to 0](../../evidence/aws/dp3-validate-log-20261028.png)

The [fact job](../../../spark_jobs/gold/build_order_snapshot_lines.py),
[SCD2 job](../../../spark_jobs/gold/build_amazon_order_status_scd2.py) and
[feature job](../../../spark_jobs/gold/build_order_snapshot_return_features.py) are submitted via
[DAG DP2](../../../dags/dp2_silver.py) / [DP3](../../../dags/dp3_features.py).
The SCD2/fact check logs show no errors; the features match the Gold key set, with no wrong event times.
There is no AWS Navigator/ERD/DESCRIBE capture of the columns yet; the PASS log does not prove every Gold modeling requirement (fact, SCD2, features, schema).
