# Data Generator data quality report (Batch)

This report records the actual measurements of the batch source and the simulated data problems. The Stream part is in [data-quality-report-stream.md](data_quality_report_stream.md).

## 100 GB scope

The main run uses `configs/scale-100gb.yaml`: the same as `default.yaml` (seed 42, 2,000 SKUs, 180 days from 2026-01-01) but with `orders_per_day = 851,506`, written to `/data/raw`. The reason for choosing 851,506 and the 100.0 GB decimal unit is in the comment at the top of the config file.

At this scope only three sources have renderers: `amazon.orders`, `walmart.orders` and `seller.skus`. TikTok Shop, Shopify and Shopee are in the manifest's `not_rendered_yet` and are generated in a small source (see the last section).

`/data/raw/run_manifest.json` records:

| Dataset | Rows | Distinct keys | Duplicate rate | Files | Bytes |
|---|---:|---:|---:|---:|---:|
| `amazon.orders` | 108,248,563 | 106,109,650 | 1.98% | 3,316 | 86,728,943,600 |
| `walmart.orders` | 10,949,058 | 10,732,935 | 1.97% | 3,255 | 13,283,621,342 |
| `seller.skus` | 2,000 | 2,000 | 0% | 1 | 225,470 |
| **Total** | | | | | **100,012,790,412** |

Total **100,012,790,412 bytes = 100.01 GB** decimal (93.14 GiB). These are the raw bytes in the manifest, confirmed to match the actual bytes on disk (`tests/unit/generator/test_generate_order_snapshots.py`), not the `df` number (which shows GiB and differs by 7.4%).

## Small run for detailed measurement

The deep-dive tables (skew by city/category, cardinality, schema evolution by day, SKU list) are measured on the `configs/default.yaml` run (4,000 Orders/day, Amazon in MinIO, Walmart in PostgreSQL) with the `report` command, because this command rereads all the data and joins with `seller.skus`. The measurement method is identical at 100 GB scale; only the absolute numbers differ. Each section below states the source of its numbers.

- Small configuration: `configs/default.yaml` (seed 42, 2,000 SKUs, 180 days from 2026-01-01, 4,000 Orders per day).
- Storage: Amazon in MinIO (bucket `platform`), Walmart and `seller.skus` in PostgreSQL (database `sources`). For the setup see [Docker.md](../docker.md).
- Measurement command:

```bash
set -a; . ./.env; set +a
export AWS_ACCESS_KEY_ID=$MINIO_ROOT_USER AWS_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD \
       FSSPEC_S3_ENDPOINT_URL=http://localhost:$MINIO_API_PORT
uv run python -m generator report --source s3://platform \
    --dsn "postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:$POSTGRES_PORT/$POSTGRES_DB"
```

The `_truth` columns are the answers the generator writes in `run_manifest.json`. Measurements are reread from the stored data, not taken from the generator's memory. The full table of the `report` command is in [generator-design.md, section Run results](generator_design.md#run-results).

## Measurement results

| Property | Status | Main measurement (100 GB) |
|---|---|---|
| Simulate skew | Measured | Prime Day 2026-04-30/05-01: 2,281,202 Amazon Orders/day vs 570,490 on a normal day (×4.0), return rate 13.7% vs 11.4%. City/category details measured on the small run |
| Simulate high cardinality | Measured | `amazon.orders`: 106,109,650 distinct Order keys over 108,248,563 rows; `walmart.orders`: 10,732,935 keys over 10,949,058 rows |
| Simulate schema evolution | Measured | `amazon.orders` switches schema exactly on export date 2026-04-10 (`proceeds` null 100% → 0%, `IsPrime` the reverse) |
| Simulate another offline problem (duplicate 2%) | Measured | Duplicates 1.98% (Amazon) and 1.97% (Walmart); most are older versions of the same key, so the latest version must be kept, not just exact copies dropped |
| Using generator configuration | Measured | Every parameter is in `configs/*.yaml`, see [section 15](generator_design.md#configuration-keys-in-use) |
| Store data (MinIO, PostgreSQL) | Measured | 100.01 GB raw: `amazon.orders` 86.73 GB in 3,316 files, `walmart.orders` 13.28 GB in 3,255 files, `seller.skus` 0.23 MB |

## Details of each problem (measured on the small run)

### Skew

```text
== storefront_skew ==
storefront_id   config_weight  orders_truth  pct_truth  orders_exported
--------------  -------------  ------------  ---------  ---------------
amazon_us       0.45           334046        45.38      334046
tiktok_shop_us  0.15           108042        14.68
amazon_uk       0.12           89332         12.14      89332
amazon_de       0.1            74778         10.16      74778
shopify         0.08           57673         7.83
walmart_us      0.07           50499         6.86       50499
shopee_vn       0.03           21759         2.96

== city_skew == (largest city per Storefront)
storefront_id  city      orders  pct
-------------  --------  ------  -----
amazon_de      Berlin    36723   49.11
amazon_uk      London    43798   49.03
amazon_us      New York  154019  46.11
walmart_us     Dallas    24843   49.2

== category_skew ==
category     order_lines  pct    return_pct_truth
-----------  -----------  -----  ----------------
apparel      140711       17.71  23.32
toys         115003       14.48  6.69
electronics  112797       14.2   8.1
accessories  105104       13.23  10.16
sports       90499        11.39  8.3
home         85100        10.71  4.57
footwear     84430        10.63  16.08
beauty       60717        7.64   5.83

== top_skus == (top 3 SKUs)
sku        order_lines  pct
---------  -----------  ----
ELE-01103  43073        5.42
TOY-01453  24485        3.08
SPT-01304  18065        2.27

== daily_orders == (Sale Event: 2026-04-30, 2026-05-01)
day         orders  x_median
----------  ------  --------
2026-05-01  10950   3.7
2026-04-30  10887   3.68
2026-03-13  3119    1.05
```

- **Storefront**: the measured Order counts match the answer for all 4 Storefronts that have renderers (e.g. `amazon_us` 334,046). TikTok Shop, Shopify and Shopee have no renderer yet, so they are left blank.
- **City**: Berlin 49.1%, London 49.0%, New York 46.1%, Dallas 49.2%. A job grouping by city will have one partition about as large as all the other partitions combined.
- **Category**: apparel 17.7% with a return rate of 23.3%, home 10.7% with a return rate of 4.6%. Category exists only in `seller.skus`, so it can only be measured by joining with PostgreSQL.
- **SKU**: `ELE-01103` accounts for 5.4% of Order Lines (43,073 rows), almost 2 times the second SKU.
- **Day**: 2026-04-30 and 2026-05-01 (Prime Day) have about 10,900 Orders, 3.7 times the median.

### High cardinality

```text
== cardinality ==
dataset         column           approx_distinct  exact_distinct  approx_err_pct  rows
--------------  ---------------  ---------------  --------------  --------------  ------
amazon.orders   order_id         385608           498156          -22.59          508227
amazon.orders   line_id          818537           721099          13.51           721099
amazon.orders   asin             1844             2000            -7.8            721099
amazon.orders   sku              1526             2000            -23.7           721099
walmart.orders  order_id         50400            50499           -0.2            51523
walmart.orders  buyerId          21549            21719           -0.78           51523
walmart.orders  line_id          68348            73262           -6.71           73262
walmart.orders  tracking_number  52956            48629           8.9             73262
```

| Column | Exact count | DuckDB HyperLogLog | Error |
|---|---|---|---|
| `amazon.orders.order_id` | 498,156 | 385,608 | -22.6% |
| `amazon.orders.line_id` | 721,099 | 818,537 | +13.5% |
| `walmart.orders.order_id` | 50,499 | 50,400 | -0.2% |

DuckDB's `approx_count_distinct` is off by more than 20% on high-cardinality columns, so the report also prints the exact count. The later Spark job will compare against this exact count.

### Schema evolution

```text
== schema_evolution_daily ==
export_date  rows  pct_null_proceeds  pct_null_IsPrime
-----------  ----  -----------------  ----------------
2026-04-07   2786  100.0              0.0
2026-04-08   2632  100.0              0.0
2026-04-09   2819  100.0              0.0
2026-04-10   2688  0.0                100.0
2026-04-11   2676  0.0                100.0
2026-04-12   2707  0.0                100.0
```

| Period | Export days | Rows | `proceeds` null | `IsPrime` null |
|---|---|---|---|---|
| Before, Orders v0 | 99 | 247,213 | 100% | 0% |
| After, Orders 2026-01-01 | 81 | 261,014 | 0% | 100% |

The boundary falls exactly on export date 2026-04-10: 2026-04-09 still has the old schema, 2026-04-10 already has the new one. `AmazonOrderId` is also renamed, so the Bronze job must merge the two schemas by the new column names.

### Duplicate 2%

```text
== duplicates ==
dataset         rows_before  distinct_keys  dup_pct_before  identical_copies  older_versions  rows_after  dup_pct_after  dup_pct_truth
--------------  -----------  -------------  --------------  ----------------  --------------  ----------  -------------  -------------
amazon.orders   508227       498156         1.98            5108              4963            498156      0.0            1.98
walmart.orders  51523        50499          1.99            0                 1024            50499       0.0            1.99
```

| Dataset | Rows | Distinct keys | Duplicates | Identical copies | Older versions | After dedup | Answer |
|---|---|---|---|---|---|---|---|
| `amazon.orders` | 508,227 | 498,156 | 1.98% | 5,108 | 4,963 | 0% | 1.98% |
| `walmart.orders` | 51,523 | 50,499 | 1.99% | 0 | 1,024 | 0% | 1.99% |

About half of Amazon's duplicates are older versions carrying an old Order Status. So dropping only identical rows is not enough; the latest update per key must be kept.

### Storage (small run)

![MinIO console](../../images/batch/minio-bucket.png)

![DBeaver](../../images/batch/dbeaver-walmart-orders.png)

![DBeaver seller.skus](../../images/batch/dbeaver-seller-skus.png)

| Dataset | Storage | Format | Files | Size |
|---|---|---|---|---|
| `amazon.orders` | MinIO | JSON Lines | 180 | 388 MB |
| `labels` | MinIO | Parquet | 1 | 13 MB |
| `walmart.orders` | PostgreSQL | table, JSONB column | | 74 MB |
| `seller.skus` | PostgreSQL | table | | 0.3 MB |

At 100 GB scope, `/data/raw` keeps the same formats but splits into more export files (3,316 Amazon files, 3,255 Walmart files) because each Export Date produces one batch; `seller.skus` is still one file. The `walmart.orders` table in PostgreSQL holds the small run's source; the 100 GB version lives in `/data/raw` as exports to avoid loading 13 GB into the database.

## Three Channels from the small source

`configs/scale-100gb.yaml` does not yet render TikTok Shop, Shopify, Shopee (see `not_rendered_yet`). So that these three Channels have full Bronze/Silver/Gold without costing 100 GB, the project runs an extra small source `configs/small.yaml` written to `out/channels-small`: 50 SKUs, 60 days of history, 200 Orders/day, seed 42. This time **all five Channels** have renderers (`not_rendered_yet` is empty):

| Dataset | Rows | Distinct keys | Duplicate rate | Files |
|---|---:|---:|---:|---:|
| `amazon.orders` | 8,999 | 8,823 | 1.96% | 666 |
| `tiktok_shop.orders` | 1,810 | 1,780 | 1.66% | 506 |
| `shopify.orders` | 910 | 891 | 2.09% | 388 |
| `walmart.orders` | 827 | 814 | 1.57% | 393 |
| `shopee.orders` | 354 | 347 | 1.98% | 226 |
| `seller.skus` | 50 | 50 | 0% | 1 |

The three rows `tiktok_shop.orders` / `shopify.orders` / `shopee.orders` (1,810 / 910 / 354) match exactly the row counts the Bronze/Silver validate jobs counted in DP1/DP2, so the pipeline for the three new Channels is checked against this small source's own numbers. This is a **deliberately small** source for checking structure and the pipeline, not evidence of reaching the 100 GB threshold; the 100 GB threshold rests only on Amazon + Walmart in `/data/raw`.

## On AWS

The batch source lives on S3 and PostgreSQL on EC2; the [AWS receipt](../../evidence/aws/data-platform-aws-20261027.json)
records `raw_s3` 17,393 objects, 141,774,479,087 bytes, matching the bytes on the EC2 disk.
The v1 Amazon + Walmart + SKU scope is 100,012,790,412 bytes, matching the local manifest;
PostgreSQL has 10,949,058 Walmart rows and 2,000 SKUs.

![DataHub filtered to AWS S3: four source datasets Amazon, Shopee, Shopify and TikTok Shop under source-exports](../../evidence/aws/datahub/s3-sources.png)

The DataHub screenshot only shows that the S3 source datasets have metadata registered; it does not show byte counts or histograms.
The byte and row counts above come from the receipt; the skew rates, unique/total, duplicates and schema before/after in the previous sections
belong to the local report. The section below remeasures those four properties **on the AWS data itself**.
[Generator code](../../../generator/__main__.py), [100 GB configuration](../../../configs/scale-100gb.yaml),
[local manifest for comparison](../../evidence/batch-100gb-manifest.json).

### Data properties measured on AWS

Measured with Trino on EC2 (catalog `lakehouse` = Iceberg S3) on the loaded 100 GB Amazon tables: Bronze
`raw_amazon_orders_dp1` (original payload) and Silver `stg_amazon_order_lines_dp2` (split into Order Lines, schema normalized).
SQL: [`sql/profile_amazon_aws.sql`](../../../sql/profile_amazon_aws.sql); full output:
[`amazon-data-profile-trino-20261031.txt`](../../evidence/aws/amazon-data-profile-trino-20261031.txt) (ran in 2 minutes 13 seconds).
Duplicate rates and sizes come from the `run_manifest.json` the generator wrote to S3 directly on EC2
(`s3://…-raw/order_snapshots/run_manifest.json`, `dataset_version` `38a1f7b3…`).

| Property | Measurement on AWS |
|---|---|
| **SKU skew** (Zipf 0.8) | 2,000 SKUs; the top 1% best-selling SKUs (20) account for **25.58%** of Order Lines, the top 10% (200) for **54.27%**; the largest SKU has 8,316,158 rows, the smallest 18,738 (444 times fewer) |
| **Marketplace skew** | US `ATVPDKIKX0DER` 67.17%, UK `A1F83G8C2ARO7P` 17.90%, DE `A1PA6795UKMFR9` 14.93% |
| **Cardinality** (unique / total, 153,849,901 Order Lines, `approx_distinct`) | `order_id` ≈ 102 million (0.66 — high), `postal_code` 12,354 (0.00008), `city` 20, `seller_sku` ≈ 2,000 |
| **Schema evolution** | Payload keys on 2026-04-08: `AmazonOrderId, FulfillmentChannel, IsBusinessOrder, IsPrime, …` (Orders API v0); on 2026-04-12: `orderId, createdTime, lastUpdatedTime, salesChannel, programs, fulfillment, proceeds, shippingAddress, orderItems` (API 2026). Silver merges them: `schema_form` `v0` 2026-01-01→04-09 = 74,821,347 rows, `2026` 04-10→06-29 = 79,028,554 rows |
| **Duplicate** (S3 manifest) | `amazon.orders` 108,248,563 rows / 106,109,650 keys = **1.98%** (1,062,953 older versions + 1,075,960 identical copies); Walmart 1.97%, Shopify 1.98%, Shopee 1.97% |
| **Volume** | Amazon 86,728,943,600 B (3,316 files), Walmart 13,283,621,342 B; total v1 source 100,012,790,412 B |

![Trino EC2: payload keys before/after the API switch, schema_form v0/2026, top 1% SKU 25.58%, marketplace 67/18/15%, cardinality order_id/postal/city/sku](../../evidence/aws/amazon-data-profile-trino-20261031.png)

Reading the numbers: the head of the Zipf distribution is very heavy (1% of SKUs hold a quarter of the rows); the skew Spark handles in [spark.md](spark.md) is skew in daily data volume, which differs from this SKU skew;
`order_id` is almost unique per Order, while `city` has only 20 values; the schema switches exactly on day 100 (2026-04-11) per the configuration
`amazon_orders_api_switch_day: 100`.
