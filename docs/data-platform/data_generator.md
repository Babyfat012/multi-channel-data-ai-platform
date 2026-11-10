# Data Generator on AWS

<a id="on-aws-2026-10-27-to-2026-10-28"></a>

This page covers generating a batch of ≥100 GB with skew, schema evolution (schema changing over time),
duplicates (repeated records), late arrival (late-arriving messages) and source persistence. The generator runs
on EC2. Batch data is stored in S3 and PostgreSQL; Stream data is published through Kafka on EC2. Configuration:
[`scale-100gb.yaml`](../../configs/scale-100gb.yaml), [`default.yaml`](../../configs/default.yaml). Code and how the data
is generated: [generator design](reference/generator_design.md).

## On AWS

The batch sources live on S3 and on the EC2 PostgreSQL. According to the [AWS receipt](../evidence/aws/data-platform-aws-20261027.json):

- `raw_s3` has 17,393 objects, 141,774,479,087 bytes in total, matching the byte count on the EC2 disk.
- The Amazon, Walmart and SKU sources alone are 100,012,790,412 bytes, matching the manifest generated locally.
- PostgreSQL has 10,949,058 Walmart rows and 2,000 SKUs.

![DataHub filtered to AWS S3: four source datasets Amazon, Shopee, Shopify and TikTok Shop under source-exports](../evidence/aws/datahub/s3-sources.png)

The DataHub screenshot only shows that the source datasets on S3 have metadata declared; it does not show byte counts or histograms.
The byte and row counts above come from the receipt. The skew, unique/total, duplicate and before/after schema figures in earlier
sections are numbers from the local report. The section below re-measures those four characteristics **on the AWS data itself**.
[Generator code](../../generator/__main__.py), [100 GB configuration](../../configs/scale-100gb.yaml),
[local manifest for comparison](../evidence/batch-100gb-manifest.json).

### Data characteristics measured on AWS

Method: run Trino on EC2 (catalog `lakehouse` is Iceberg on S3) against the loaded 100 GB Amazon table. Two tables
are read:

- Bronze `raw_amazon_orders_dp1`: the raw payload.
- Silver `stg_amazon_order_lines_dp2`: split into individual Order Lines (product rows in an order) with a normalized schema.

SQL: [`sql/profile_amazon_aws.sql`](../../sql/profile_amazon_aws.sql). Full output:
[`amazon-data-profile-trino-20261031.txt`](../evidence/aws/amazon-data-profile-trino-20261031.txt) (total run time 2 minutes
13 seconds). The duplicate rate and sizes come from `run_manifest.json`, the file the generator writes to S3 while running on EC2
(`s3://…-raw/order_snapshots/run_manifest.json`, `dataset_version` `38a1f7b3…`).

| Characteristic | Measured on AWS |
|---|---|
| **SKU skew** (Zipf distribution 0.8) | 2,000 SKUs. The top 1% best-selling SKUs (20 SKUs) account for **25.58%** of Order Lines; the top 10% (200 SKUs) for **54.27%**. The largest SKU has 8,316,158 rows, the smallest 18,738 rows (a 444× difference) |
| **Marketplace skew** | US `ATVPDKIKX0DER` 67.17%, UK `A1F83G8C2ARO7P` 17.90%, DE `A1PA6795UKMFR9` 14.93% |
| **Cardinality** (distinct values / total rows, over 153,849,901 Order Lines, approximate counts with `approx_distinct`) | `order_id` ≈ 102 million (ratio 0.66, high), `postal_code` 12,354 (0.00008), `city` 20, `seller_sku` ≈ 2,000 |
| **Schema evolution** | On 2026-04-08 the payload has the keys `AmazonOrderId, FulfillmentChannel, IsBusinessOrder, IsPrime, …` (Orders API v0). On 2026-04-12 the payload changes to `orderId, createdTime, lastUpdatedTime, salesChannel, programs, fulfillment, proceeds, shippingAddress, orderItems` (API 2026). Silver reads both forms: `schema_form` `v0` from 2026-01-01 to 04-09 has 74,821,347 rows; `2026` from 04-10 to 06-29 has 79,028,554 rows |
| **Duplicates** (per the manifest on S3) | `amazon.orders` has 108,248,563 rows over 106,109,650 keys, so **1.98%** are duplicates (1,062,953 older versions of the same order and 1,075,960 exact copies). Walmart 1.97%, Shopify 1.98%, Shopee 1.97% |
| **Volume** | Amazon 86,728,943,600 B (3,316 files), Walmart 13,283,621,342 B. Total of the Amazon, Walmart and SKU sources is 100,012,790,412 B |

![Trino EC2: payload keys before/after the API change, schema_form v0/2026, top 1% SKU 25.58%, marketplace 67/18/15%, cardinality order_id/postal/city/sku](../evidence/aws/amazon-data-profile-trino-20261031.png)

How to read the numbers above:

- The head of the Zipf distribution is very heavy: just 1% of SKUs hold a quarter of the rows.
- The skew Spark handles in [spark.md](reference/spark.md) is daily volume skew, which is different from the SKU skew here.
- `order_id` is almost unique per Order, while `city` has only 20 values.
- The schema changes exactly on day 100 (2026-04-11), exactly as configured by `amazon_orders_api_switch_day: 100`.

## On AWS

This Stream run: the generator publishes into Kafka (running with Compose on EC2) with `configs/default.yaml`, seed 42,
`speed=0` (publish as fast as possible), in 41 seconds. The numbers in the table below come from the
[AWS manifest](../evidence/aws/stream-manifest-20261028.json). This is a separate cloud run and does not use the
`STREAM_SPEED=60` pace of the local run above.

| Characteristic | Amazon notification | TikTok webhook |
|---|---|---|
| Rows / unique keys | 399,410 / 399,410 | 93,554 / 92,157 |
| Duplicates | 0 | 1,397, rate 0.014933 ≈ 1.49% |
| Median delay | 5.27 minutes | 0.06 minutes |
| Share of messages more than 1 hour late | 0.049763 ≈ 4.98% | 0 |
| Out-of-order messages by more than 1 hour | 19,729 | 0 |

Amazon delay is the gap between `EventTime` and the time the message is received. TikTok duplicates keep the same
`tts_notification_id` as the original message. Livestream burst (a sales surge during a livestream): 148.5 Orders/hour versus 25.2
Orders/hour normally, 5.89× according to the manifest.
[Generator code](../../generator/__main__.py), [configuration](../../configs/default.yaml),
[measurement script](../../scripts/evidence/measure_order_snapshots_stream.py).

![Flink 1.20.3 Completed Jobs: baseline Amazon, late Amazon, two dedup jobs and the window job all FINISHED](../evidence/aws/flink-completed-jobs-20261028.png)

![Flink dedup_tiktok_state_proof: HASH edge to Deduplicate, source sent 93,554 records, job FINISHED](../evidence/aws/flink-dedup-graph-20261028.png)

The screenshots show that the Flink consumers finished and the dedup job received 93,554 records. The lateness and duplicate characteristics in the table come
from the manifest, not from the screenshots. Flink separated 10,166 late messages, fewer than the generator counted, because the watermark (the point in time
Flink considers "fully received") advances periodically and per partition; see
[processing and job_id](reference/flink.md#on-aws). There is also AWS output from the continuously running late job: 6,674/6,674
rows satisfy `event_time <= watermark`, with examples of late messages, see
[Processing Jobs](processing_jobs.md#flink-baseline-late-arrival-dedup-and-window). No AWS Flink UI screenshot with the watermark
operator opened yet.

[Section index](README.md) · [Main index](../README.md)
