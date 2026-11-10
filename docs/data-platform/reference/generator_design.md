# Data Generator Design

Design and implementation document for the project's simulated data generator. Status: **implemented within the scope described in section 15**. For terminology (Channel, Storefront, Order Line, ...) see [Domain.md](../../shared/domain.md). For the sources and confidence level of each Channel's data shape, see [channel-sources.md](channel_sources.md).

Resume update: before any write/delete, Batch compares
`_dataset_identity.json` against the config (excluding the output root), the Storefront order,
the SHA-256 of the generator code and the Python/NumPy/PyArrow/fsspec versions. Checkpoints and the
manifest both carry `dataset_version`. Changing config/code/runtime, or an old root
without an identity, fails; a new identity is never attached to old data automatically.
Use a new root when a different world is needed. `--restart` keeps its meaning of regenerating the data
the generator owns in the chosen root, so only use it when you deliberately want to redo it.
Do not run restart on an evidence set that must be kept. The guard assumes a single writer;
it is not yet a distributed lock or a hash check over every generated file.
The fix records tests and limitations.
[Source contract v2](source_contract_order_lifecycle.md) is the next target; it is not
enforced just by having `dataset_version`; the current label logic is still v1.

## Table of contents

1. [Goals and scope](#1-goals-and-scope)
2. [Three-layer architecture](#2-three-layer-architecture)
3. [Timeline: Batch and Stream](#3-timeline-batch-and-stream)
4. [Generating the Return/Refund label](#4-generating-the-returnrefund-label)
5. [Injected data problems](#5-injected-data-problems)
6. [Data sources per Channel](#6-data-sources-per-channel)
7. [Reference data](#7-reference-data)
8. [Storage layout](#8-storage-layout)
9. [Buyers, currency and time](#9-buyers-currency-and-time)
10. [Configuration](#10-configuration)
11. [Answer key and quality evidence](#11-answer-key-and-quality-evidence)
12. [Engineering and packaging](#12-engineering-and-packaging)
13. [Assumptions and open items](#13-assumptions-and-open-items)
14. [Design and evidence](#14-design-and-evidence)
15. [Implementation](#15-implementation)

## 1. Goals and scope

The generator produces sales data for one Seller (ACME) across 7 Storefronts on 5 Channels, so that the downstream components (Bronze, Silver, Gold, Feature Store, drift monitoring) have data with real problems to handle.

Defaults: 2,000 SKUs, 180 days of history, about 4,000 Orders per day (about 720 thousand Orders, more than 1 million Order Lines). Every number is configurable; see [section 10](#10-configuration).

Out of scope for this phase: fees, settlement, advertising, FBA inventory, clickstream.

## 2. Three-layer architecture

```
Simulation core ──► Per-Channel renderer ──► Faulty delivery layer ──► MinIO / PostgreSQL / Kafka
  (the truth)        (each platform's        (duplicates, late,
                      native shape)            batches)
```

1. The **simulation core** generates the shared truth, without any Channel's shape: SKU, Listing, Order, Order Line, Return, Refund, buyer, `Delivered At`, following the canonical Order Status.
2. The **per-Channel renderer** turns the truth into the Channel's native shape: field names, status names, file format, exposing only what the Channel actually shows the Seller.
3. The **faulty delivery layer** injects faults last: duplicate, late, batched, burst.

Why split into layers:
- There is a shared answer key to check Silver results against (canonical Order counts, true labels).
- The label-generation rule lives in one place, not repeated for 5 Channels.
- Faults are injected last, so exactly how many were injected is known and recorded in `run_manifest.json`.

The trade-off is writing an extra renderer layer compared with generating each Channel directly.

## 3. Timeline: Batch and Stream

`cutoff` is the "today" mark in the simulated world. The simulation core runs **continuously** across `cutoff`:

```
day 1 ───────────── day 180 (cutoff) ───────────── day 181 → 210
       BATCH                                STREAM
```

- **Batch**: everything that happens before `cutoff`, each Channel in its native shape, plus a status snapshot of the Orders still open at `cutoff`.
- **Stream**: the simulation continuing after `cutoff`, including new Orders, Order Status changes of Orders already in the Batch, and late Returns.
- An Order seen in the Batch keeps being updated in the Stream because it is the same simulation.
- Labels of Orders delivered in the last 30 days before `cutoff` are unknown at `cutoff`, so they are excluded from the training set. Labels arrive gradually in the Stream.
- Simulated days map to the real calendar through `start_date` in the configuration.
- The Stream emission speed is configurable per range of days (see [section 5](#streaming)), with a fast mode for testing.

Why not replay the Batch data itself: there would be no new Orders, no late-arriving labels, and no drift after the model has been trained.

## 4. Generating the Return/Refund label

Label: whether an Order Line is Returned or Refunded within 30 days of delivery.

Simulated data has no real behavior, so the core uses a structured probabilistic rule that a model can learn instead of guessing blindly:

- The return probability is a logistic function of: category, Storefront, Fulfillment Method, discount level, price, quantity, and **SKU Defect Propensity**.
- **SKU Defect Propensity** is a hidden per-SKU "defect level" (for example `HOODIE-BLK-M` runs small, so it is returned a lot on every Storefront). It never appears as a column and can only be inferred from history. This is exactly what the Feature Store looks up by `sku_id` + `storefront_id`.
- The delay from delivery to return is drawn from a per-Channel distribution, up to about 30 days, with a rare tail beyond 30 days.
- **No leakage**: no column at Order creation time reveals the label.
- Target rates: base level about 10 to 12%, apparel about 22%, home goods about 4%.
- Return reasons: each Channel uses its own native reasons (TikTok Shop `return_reason`, Shopee `reason`, Walmart reasons in `returnOrderLines`). Amazon alone uses about 10 codes (`WRONG_SIZE`, `DEFECTIVE`, `NOT_AS_DESCRIBED`, `CHANGED_MIND`, `ARRIVED_LATE`, `DAMAGED_IN_TRANSIT`, `WRONG_ITEM_SENT`, `BETTER_PRICE_FOUND`, `QUALITY_NOT_EXPECTED`, `ACCIDENTAL_ORDER`). The Amazon list is an **assumption** because no official list is available.

This rule is an assumption of the generator. A model that does well on simulated data does not prove it does well in the real world.

### Delivery date (`Delivered At`)

The label is computed from the core's **true** `Delivered At`. Each Channel only exposes what it actually shows the Seller:

| Channel | What the Seller sees |
|---|---|
| TikTok Shop, Walmart | `DELIVERED` status with the update time |
| Shopify | Fulfillment event |
| Amazon MFN | Only the expected delivery date: `EarliestDeliveryDate`/`LatestDeliveryDate` in Orders |
| Amazon FBA | Only the expected delivery date: `estimated-arrival-date` in the FBA Shipments report |
| Shopee | Only `update_time` when moving to `COMPLETED` |

Pipelines and features only see the date the Channel provides, while the label uses the true date to have a correct answer key. Amazon and Shopee are assumptions; see [channel-sources.md](channel_sources.md).

## 5. Injected data problems

### Offline (Batch)

| Problem | How it is injected |
|---|---|
| **Skew** | Orders by Storefront: Amazon US 45%, TikTok Shop US 15%, Amazon UK 12%, Amazon DE 10%, Shopify 8%, Walmart US 7%, Shopee VN 3%. SKUs follow a Zipf distribution, with the parameter chosen so the best-selling SKU accounts for about 5% of Order Lines, giving the Spark job a clearly slow task. 8 categories with different return rates. Skewed geography: a few large cities account for most of each Storefront's Orders. Prime Day multiplies the Order volume on some days |
| **High cardinality** | Keep 2,000 SKUs. High cardinality comes from `order_id` (about 720 thousand), `order_line_id` (more than 1 million), `tracking_number` (about 700 thousand), and per-Channel buyer IDs (see [section 9](#9-buyers-currency-and-time)) |
| **Schema evolution** | From a fixed date, Amazon order data switches from the Orders v0 model to Orders 2026-01-01. Both are official models. The 2026 shape has no new fields — every key maps back to a v0 key: renames (`AmazonOrderId` to `orderId`, `PurchaseDate` to `createdTime`, `OrderTotal` to `proceeds.grandTotal`, nested one level deeper), restructuring (`OrderStatus` to `fulfillment.status`, value `Shipped` to `SHIPPED`), type changes (`IsPrime` bool to a `programs` array; no information is lost because the array is empty when not Prime), and dropped keys (5 keys exist only in v0). Because key names change, a column that exists in only one shape is null in the other: `proceeds` is null in v0 partitions, `is_prime` is null in 2026 partitions, `programs` is null in v0 partitions |
| **Duplicate 2%** | Overlapping export days (for example re-pulling the last 3 days to catch updates, like Amazon's `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL` report). There are identical copies and **revised copies** (same key, newer update time, newer status). Dedup must keep the latest copy, not just drop identical copies |

How the duplicate rate is measured: `1 - count(distinct key) / count(*)`. Key per Channel: Amazon `AmazonOrderId` (`orderId` after the schema change), TikTok Shop `id`, Shopify `id`, Walmart `purchaseOrderId`, Shopee `order_sn`. The update-time column used to pick the latest copy: Amazon `LastUpdateDate`/`lastUpdatedTime`, TikTok Shop and Shopee `update_time`, Shopify `updated_at`, Walmart `fetched_at`.

### Streaming

| Problem | Channel | Default level |
|---|---|---|
| **Duplicate** | TikTok Shop, Shopify (webhook) | 1.5%. Duplicates keep the **same** notification ID (TikTok Shop `tts_notification_id`) so Flink can dedup them |
| **Late** | Amazon (notification) | Median delay of a few minutes, 5% delayed more than 1 hour. Late messages are sent after messages with a later `event_time`, i.e. they arrive out of order |
| **Batched** | Walmart, Shopee (polling, a design choice of the project) | Grouped in cycles of 5 to 15 minutes |
| **Burst** | TikTok livestream (after `cutoff`) | 5 to 8 times the normal rate for a few hours |

Prime Day also causes a burst, but it is in the Batch, so it only counts toward daily skew, not toward the stream burst.

**Segmented emission speed.** A burst must last long enough in wall-clock time to be visible in the Flink UI (backpressure, throughput). If every simulated day were emitted evenly over 60 real seconds, a 6-hour livestream would last only about 15 seconds. So the Stream is emitted in segments:
- Normal days: 60 real seconds per simulated day.
- Livestream days: 600 real seconds per simulated day. A 6-hour livestream lasts about 150 real seconds, with a message rate 5 to 8 times normal.
- A full emission of 30 Stream days takes about 40 minutes.

Late arrival is not affected by the emission speed, because Flink measures delay against `event_time` in the payload.

### Drift

| Type | Event | Where |
|---|---|---|
| **Covariate** | Prime Day: deep discounts and a shifted SKU distribution on Amazon | In the Batch, around day 120. The training set has already seen one sale |
| **Concept** | TikTok livestream: the apparel return rate nearly doubles, features unchanged | Only in the Stream, after `cutoff`. The model has never seen it and must detect it before labels arrive 30 days later |

Drift is a change of core parameters on a configured schedule, not an edit of the data after generation.

## 6. Data sources per Channel

Each source matches official documentation. Fields with "Medium" or "Low" confidence are recorded as assumptions; see [channel-sources.md](channel_sources.md).

| Channel | Batch source | Stored in | Stream |
|---|---|---|---|
| Amazon | (1) Order JSON, following Orders v0 then Orders 2026-01-01. (2) Returns report TSV `GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE` for MFN orders. (3) FBA Shipments report `GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL` (TSV). (4) FBA Returns report `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` (TSV) | MinIO | Notifications, often late |
| TikTok Shop | (1) Order JSON following Get Order Detail 202309. (2) Return JSON following Search Returns 202602 | MinIO | Webhook type 1 (order status change) and 12 (return status change), often duplicated |
| Shopify | Order JSONL following the REST Order structure, including `refunds` and `fulfillments` (REST is legacy, kept because the fields were verified) | MinIO | Webhook, often duplicated |
| Walmart | Order and return JSON in a `raw_payload` JSONB column, with `fetched_at` | PostgreSQL | Polling, arrives in batches |
| Shopee | Order (`get_order_detail`) and return (`get_return_list`) JSON in a `raw_payload` JSONB column, with `fetched_at` | PostgreSQL | Polling, arrives in batches |

Notes:
- Amazon has 4 sources with different formats, so Silver must join them by `order-id`.
- Bronze has two ingest styles: reading files from MinIO and reading over JDBC from PostgreSQL.
- A detail kept as documented to force the status-mapping layer to handle it: TikTok Shop uses `CANCELLED` in the API but `CANCEL` in webhooks.

## 7. Reference data

Later parts need dimension tables with history (SCD2) and dim–fact relationships, so the generator also exports:

| Data | Content | Stored in | Used for |
|---|---|---|---|
| SKU catalog | `sku_id`, name, category, cost price. Simulates the Seller's ERP system | PostgreSQL, `seller.skus` | `dim_sku` |
| Daily Listing snapshot | Each Listing each day: Storefront, the Channel's Listing ID (ASIN, variant ID, ...), price, status. Prices change around sales, so Prime Day naturally produces many new versions | MinIO | `dim_listing` SCD2 |
| Daily exchange rates | USD rates for GBP, EUR, VND by day | MinIO | Converting money to USD |

The Listing snapshot is in a common shape exported by the Seller, not in each Channel's format. This is an assumption to keep the scope bounded.

## 8. Storage layout

- **MinIO**, bucket `platform`, prefix `source-exports/`: paths by source and then by **export date**, for example `source-exports/channel=amazon/dataset=orders/export_date=2026-03-14/`. The same bucket also holds `pg-load/` (rows waiting to be loaded into PostgreSQL), `ground-truth/` and `run_manifest.json`. One bucket is used because the manifest has to live in some bucket; see [Docker.md](../docker.md#1-source-environment-with-docker-compose). Partitioning is by export date (not order date) because overlapping exports and schema changes are both tied to the export date, and the DP1 pipeline pulls data day by day. Each day has a few medium-sized files. Some days deliberately generate a large number of small files to create a small-file problem for the Lakehouse part.
- **PostgreSQL** (simulating another department's system): one schema per source, including `walmart.orders`, `walmart.returns`, `shopee.orders`, `shopee.returns`, `seller.skus`. Order tables keep the API body in a `JSONB` column next to a few key columns, like a system that stores API responses. Order tables have no primary key, because duplicates must stay intact for the downstream pipeline to dedup. The `raw_` prefix is reserved for the Bronze layer and not used in the source system.
- **Kafka**: one topic per Channel. The key is the Order ID so that events of the same Order go to the same partition. The value is the Channel's native payload (for example the TikTok Shop webhook body).

### File names in an `export_date=` directory

Each `export_date=` directory contains **many** files, one per simulated day:

```
source-exports/channel=amazon/dataset=orders/export_date=2026-03-14/
    part-sim00039.jsonl      ← rows written by simulated day 39
    part-sim00040.jsonl
    ...
```

Why not use one shared file: an Order is exported on the day of its **last status
change**, up to about 20 days after its creation date (measured maximum 18 across
736,129 Orders; the hard ceiling from the lifecycle constants is `handling` ≤ 3 + `transit` ≤ 6 +
`completed` ≤ 7 = 16 days, plus `overlap_days` = 3 for duplicates). So a shared file
would have to stay open for ~20 simulated days and receive rows from all 20 of those days — and at no
point could anyone say "day d is done and fully on disk".

Partitioning is unchanged: the directory is still the export date, so Spark, Iceberg, Trino and
`generator/report.py` receive exactly the same set of rows as when each directory had a single file.

### Resuming after an interruption

The run that reaches the 100 GB threshold takes about 3.2 hours on one core, so it has to survive
being killed midway.

| Component | Role |
|---|---|
| `_checkpoint/day-NNNNN.json` | Both the "day NNNNN is done" marker and the holder of every counter accumulated up to the end of that day. Combined in one file so the marker and the numbers never drift apart |
| `part-simNNNNN.*` | Files written by day NNNNN. Without the marker for day NNNNN, these files are partial writes |

The write order is critical:

```
close file  →  fsync (force to the physical disk)  →  ONLY THEN write the marker (write to temp, then rename)
```

Doing it the other way round, a power loss could leave a marker saying "day d is done" while day d's file
is missing its tail; the next run trusts the marker, skips day d, and data is missing **while the manifest still
adds up to nice numbers, with no error reported**.

At startup: find the largest `d` such that the markers of all days `1..d` exist (not the largest
number found — an isolated marker after a gap proves nothing), load the counters
from that marker, delete every `part-sim` of days `> d`, then run from `d+1`. The `--restart` flag
ignores markers and regenerates from day 1.

This is correct because each simulated day draws its own random numbers from
`(seed, "batch_exports", day)` and does not read the previous day's state
(`tests/unit/generator/core/test_simulate.py::test_a_day_does_not_depend_on_previous_days`), so day `d` produces
the same data whether the run started at day 1 or jumped in at day `d`. `tests/unit/generator/test_generate_order_snapshots_resume.py`
locks in that promise by comparing byte for byte a continuous run and an interrupted one.

## 9. Buyers, currency and time

- The core knows the full buyer (ID, country, postal code). Renderers only expose what the Channel really exposes:

  | Channel | Buyer ID | Notes |
  |---|---|---|
  | Walmart | `buyerId` | Has `isGuest` |
  | Shopify | `customer.id` | Null for guests who are not logged in |
  | TikTok Shop | `user_id` | Only meaningful within TikTok Shop |
  | Shopee | `buyer_user_id` | Only meaningful within Shopee |
  | Amazon | No stable buyer ID | Only restricted shipping information |

  No ID can be shared across Channels, so features still look up by `sku_id` + `storefront_id`.
- Money keeps the original amount with the original currency code (USD, GBP, EUR, VND by Storefront) and is converted to USD with the daily exchange-rate table in [section 7](#7-reference-data). Rates are generated from a simulated series fluctuating slightly around the base value. Shopify keeps both the store currency (`currency`) and the currency the customer paid in (`presentment_currency`).
- Times are stored in UTC.
  - `event_time` is when the event happened, already present in the native payload (for example TikTok Shop's `update_time`, Amazon's `LastUpdateDate`).
  - `ingested_at` is when the system received it. For the stream it is the Kafka record timestamp when the generator sent it. For the Batch it is `export_date` or `fetched_at`.
  - The difference between the two is the late arrival.

## 10. Configuration

All parameters live in one YAML file. The key names and values below are a **proposal** for the whole generator. For the keys that already have code, with the meaning of each key, see [section 15](#configuration-keys-in-use).

```yaml
seed: 42
scale:
  n_skus: 2000
  start_date: 2026-01-01     # simulated day 1
  days_history: 180          # from day 1 to cutoff
  days_stream: 30            # after cutoff
  orders_per_day: 4000
  sku_zipf_exponent: 0.8     # best-selling SKU about 5% of Order Lines (with 2,000 SKUs)
storefront_weights:          # skew by Storefront
  amazon_us: 0.45
  tiktok_shop_us: 0.15
  amazon_uk: 0.12
  amazon_de: 0.10
  shopify: 0.08
  walmart_us: 0.07
  shopee_vn: 0.03
labels:
  base_return_rate: 0.11
  window_days: 30
offline_problems:
  duplicate_rate: 0.02
  overlap_days: 3
  amazon_orders_api_switch_day: 100    # v0 to 2026-01-01
  small_file_days: [45, 46, 47]
streaming_problems:
  pacing:
    default_seconds_per_sim_day: 60
    segments:                          # emit slowly around events to observe
      - {from_day: 195, to_day: 195, seconds_per_sim_day: 600}
  duplicate_rate: {tiktok_shop_us: 0.015, shopify: 0.015}
  amazon_late: {median_minutes: 5, share_over_1h: 0.05}
  polling_batch_minutes: [5, 15]
  burst_multiplier: [5, 8]
drift:
  prime_day: {day: 120, days: 2}
  tiktok_live: {day: 195, start_hour_utc: 12, duration_hours: 6}
storage:
  minio_bucket: source-exports
  postgres_channels: [walmart_us, shopee_vn]
```

Every number can be increased when running in the cloud (for example `n_skus`, `orders_per_day`). `sku_zipf_exponent` needs re-tuning after measurement to hit the target of about 5%.

## 11. Answer key and quality evidence

### `run_manifest.json`

After each run, the generator writes `run_manifest.json`: seed, configuration, row counts per source, the actual injected fault rates (duplicate, late, burst), the schema change date, the drift events, and size and file count per source. This is the answer key to check against.

### Label table (`labels`)

The generator exports a separate table of true labels, not only recorded in the manifest, so it can be joined by ID with the feature table during training.

| Column | Meaning |
|---|---|
| `order_line_id` | The core's canonical Order Line ID |
| `label` | 1 if the Order Line was Returned or Refunded within 30 days of `delivered_at`, otherwise 0 |
| `delivered_at` | True delivery date (UTC), for point-in-time joins |
| `label_available_at` | `delivered_at` + 30 days, when the label actually becomes known |

- Only delivered Order Lines are included. Orders canceled before delivery have no label.
- The Batch table only contains rows with `label_available_at` before `cutoff`. The remaining rows get labels when the Stream is emitted.
- Stored as Parquet at `ground-truth/labels/` in MinIO, separate from the `source-exports/` prefix because it is not any Channel's data.
- To join with Channel data, the auxiliary table `ground-truth/order_line_keys/` maps `order_line_id` to the Channel's native ID (for example Amazon's `OrderItemId`).

### Data quality report

The data quality report has two parts:

- **The `generator/report.py` module** holds the measurement logic, with docstrings and unit tests, using DuckDB (lightweight, has `approx_count_distinct`, can read both MinIO and PostgreSQL). With `--dsn`, Walmart and the SKU catalog are read from PostgreSQL; without it, the `pg-load/` files are read.
- **The `python -m generator report` command** prints the measurement tables to the terminal for screenshots, or as Markdown (`--markdown`) to paste into documents. A charting notebook is deferred: it needs Jupyter and a plotting library, while text tables are enough for screenshots.
- **Stream** has two sources of numbers: `stream_manifest.json` holds the totals of an emission, while `scripts/evidence/measure_order_snapshots_stream.py` replays the Stream in memory and prints a concrete example of each problem (a pair of duplicate messages, the most delayed message, a table of Orders per hour on the livestream day).
- **Screenshots and reports** are split by timeline, each side with its own document and image directory: Batch in [data-quality-report-batch.md](data_quality_report_batch.md) with `docs/images/batch/`, Stream in [data-quality-report-stream.md](data_quality_report_stream.md) with `docs/images/stream/`. Each image comes with an explanation of what it proves.

| Property to measure | What the report measures |
|---|---|
| Skew distribution (city/category %) | Percentage of Orders by city, by category, by Storefront, by top SKU, by day |
| Cardinality | `approx_count_distinct` of `order_id`, `order_line_id`, `tracking_number`, buyer ID |
| Schema evolution | Null rate of the `proceeds` column (only present in the 2026 shape, because key names changed) per `export_date`, before and after the schema change date |
| Duplicate before and after dedup | Duplicate rate using the formula in section 5, before and after applying the dedup rule (keep the latest copy per key). The report computes both itself so the generator side does not depend on Spark. The later Spark job must produce the same numbers |
| Streaming burst/late/duplicate | Messages per minute, distribution of `ingested_at - event_time`, rate of repeated `tts_notification_id` |
| Data volume, format after storage | Size and file count per source, format of each source |

Each measured number is placed next to the number in `run_manifest.json`.

## 12. Engineering and packaging

- Python, random numbers via `numpy` `Generator` with a single `seed`. The same seed and configuration give the same data. Only the time Kafka receives a message depends on the real clock.
- Two run modes: generate the Batch (a one-off job) and emit the Stream (a long-running process sending to Kafka).
- Output is written through `fsspec`/S3 API to MinIO or a cloud bucket, not through a personal machine's disk.
- Work is split by day to save RAM. More than 1 million Order Lines run in one process; Spark is not needed yet.
- Local unit tests only run a very small configuration (for example 20 SKUs, 3 days).
- Every file has a description at the top, and every function and class has a docstring, following the project's general documentation convention.
- **Docker**: there is a Docker Compose setup (MinIO, PostgreSQL, Kafka) for the test environment. To get "reduced from X to Y" numbers, first build a simple image (single stage, full base image) and record its size, then build the optimized one (multi-stage, slim base image, no install cache kept) and record its size. Results are recorded in `docs/data-platform/docker.md`.
- Heavy infrastructure runs in the cloud, provisioned with Terraform and `destroy`ed when idle.

## 13. Assumptions and open items

**Assumptions** (stated explicitly, to be verified if a source becomes available):
- The label-generation rule and its coefficients.
- The list of about 10 Amazon return reason codes.
- The delivery date of Amazon and Shopee (only a proxy is visible).
- The `GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE` report only contains MFN orders; FBA returns are in the FBA Returns report.
- The date Amazon switches from Orders v0 to Orders 2026-01-01 is self-chosen, not Amazon's real schedule.
- Shopee `ReturnStatus` values other than `REQUESTED` and `CANCELLED`.
- Walmart's list of return statuses, and the full column lists of the FBA Shipments and FBA Returns reports.
- Amazon notifications and Shopify webhooks: the payload and how the Channel sends duplicates have not been read from official documentation.
- The Listing snapshot in a common shape exported by the Seller.

**Open items:**
- The status of Shopify REST and GraphQL in 2026.
- Size measured in the source manifest: 100,012,790,412 bytes for Amazon/Walmart/SKU; see the [Batch report](data_quality_report_batch.md).
- Spark scale has been checked with `configs/scale-100gb.yaml`; the 20/180-day benchmark is in [Spark](spark.md), and the Airflow pipeline uses a smaller window recorded in [Airflow](../pipeline_orchestration.md). The old `large.yaml` plan is not used.
- Bronze/Silver for all five Channels, Gold facts, Amazon SCD2 and offline features have run; schema and evidence scope are in [Schema](../schema_design.md) and [Gold](gold.md).

## 14. Design and evidence

| Topic | Design | Evidence / how to verify |
|---|---|---|
| Docker & Docker Compose | Section 12 | `docs/data-platform/docker.md` |
| Optimize Dockerfile | Section 12 | Image size before and after optimization |
| Simulate skew | Section 5, offline | Report screenshot, section 11 |
| Simulate high cardinality | Section 5, offline | Screenshot of `approx_count_distinct` |
| Simulate schema evolution | Section 5, offline | Screenshot of null rate per `export_date` |
| Simulate another offline problem | Section 5, duplicate 2% | Screenshot of duplicate rate before and after dedup |
| Using generator configuration | Section 10 | Configuration file and per-key documentation |
| Store data for ingestion into Bronze | Sections 6, 7, 8 | Screenshots of MinIO and PostgreSQL |
| Simulate burst | Section 5, streaming | Screenshot of messages per minute |
| Simulate late arrivals | Section 5, streaming | Screenshot of the delay distribution |
| Simulate another streaming problem | Section 5, duplicate 1.5% | Screenshot of the repeated notification ID rate |
| Using generator configuration (streaming) | Section 10, `streaming_problems` | Configuration file |
| Document data characteristic | Sections 6, 8, 11 | Size, file count, format from the report |
| Document config generator | Section 10 | Per-key documentation |

The following parts use the generator's data: SCD2 dimensions and dim–fact relationships (section 7), the Lakehouse small-file problem (section 8), Flink windows (section 5, streaming).

## 15. Implementation

This section records what already has code: where the code lives, how to run it, and why it is written that way. Each file in `generator/` has a description at the top that explains in more detail.

### Progress

| Step | Content | Status |
|---|---|---|
| 1 | Simulation core, Amazon Orders (with schema change), Walmart Orders, duplicates from overlapping exports, label table, manifest, Prime Day-style Sale Event | Done |
| 2 | Docker Compose (MinIO, PostgreSQL), `seller.skus` table, PostgreSQL load command, `report.py`, optimized Dockerfile | Done (notebook deferred) |
| 2b | Amazon Returns and FBA reports, small-file, Listing, exchange rates | Not done |
| 3 | Stream through Kafka: burst (TikTok livestream), late (Amazon), duplicate (TikTok Shop), segmented emission pacing | Done. Shopify, Walmart and Shopee in the Stream deferred to step 4 |
| 4 | TikTok Shop, Shopify, Shopee, drift | Three Batch renderers and Bronze/Silver/Gold have run on a small source; Stream for the three Channels and concept drift not done |

### Trying it out

```bash
uv sync                                                   # install libraries into .venv
uv run pytest -q                                          # run tests, a few seconds
uv run python -m generator batch --config configs/small.yaml     # small data, about 1 second, written to ./out/small
uv run python -m generator batch --config configs/default.yaml --output s3://...   # full scale
uv run python -m generator report --source ./out/small                # quality report from local files
```

To run the whole source environment (MinIO, PostgreSQL, generate and load data) with Docker Compose, see [Docker.md](../docker.md).

With the full configuration (2,000 SKUs, 180 days, 4,000 Orders per day), one run on a personal machine takes about 82 seconds, uses at most about 800 MB of RAM, and writes about 485 MB.

### Code map

The code is split exactly along the three layers in [section 2](#2-three-layer-architecture). Reading in the order below is easiest.

| File | Layer | Job |
|---|---|---|
| `generator/config.py` | | Reads YAML into dataclasses, errors on mistyped keys |
| `generator/core/storefronts.py` | Core | Constants: 7 Storefronts (currency, cities, Channel-fulfilled share), 8 categories (target return rate, price range) |
| `generator/core/rngs.py` | Core | A separate random number generator for each part, so the same seed always gives the same data |
| `generator/core/catalog.py` | Core | Generates SKUs, SKU Defect Propensity, Zipf popularity and the sale-day distribution |
| `generator/core/model.py` | Core | `Order`, `OrderLine`, `Buyer`, and `Order.status_at(t)` |
| `generator/core/returns.py` | Core | Logistic function for return probability, return delay, label |
| `generator/core/simulate.py` | Core | Generates Orders day by day with their entire lifecycle |
| `generator/delivery/batch_exports.py` | Delivery | Which export date an Order lands in, duplicate injection |
| `generator/channels/amazon.py`, `walmart.py` | Renderer | Draws Orders as the Channel's native records |
| `generator/channels/tiktok.py` | Renderer | TikTok Shop webhook type 1 (order status change) for the Stream |
| `generator/delivery/stream_events.py` | Delivery | Turns status changes after `cutoff` into messages: arrival time, lateness, duplicates, pacing |
| `generator/publish_order_snapshot_events.py` | | The `generator.publish_order_snapshot_events` command: emits to Kafka or to files, measures and writes `stream_manifest.json` |
| `generator/generate_order_snapshots.py` | | Wires the three layers together, writes files, writes the manifest |
| `generator/storage.py` | | Writes and reads a local directory or MinIO through fsspec |
| `generator/load_postgres.py` | | The `load-postgres` command: loads `pg-load/` into `walmart.orders` and `seller.skus` with `COPY` |
| `generator/report.py` | | The `report` command: re-measures Batch data quality with DuckDB, next to the answer key in the manifest |
| `scripts/evidence/measure_order_snapshots_stream.py` | | Replays the Stream in memory, printing concrete examples of duplicates, lateness and burst for screenshots |

### Four key ideas of the approach

**1. The core generates the Order's whole future; the renderer only "takes a snapshot".** When an Order is created, the core immediately decides when it is paid, shipped, delivered, and whether it is returned. The result is stored as a `timeline`, the list of Order Status changes. The renderer calls `order.status_at(t)` to know what the Order looks like at time `t`. This way the same Order can be exported on several export dates with different statuses, and later the Stream only needs to keep taking snapshots after `cutoff`.

**2. Each Order is exported exactly once, and only then are duplicates injected.** An Order's primary copy is on the export date containing its last update before `cutoff`. Before faults are injected, the per-key duplicate rate is 0, so the measured duplicate rate equals exactly what was injected. With probability `p = r / (1 - r)` (r is `duplicate_rate`), an Order gets one extra copy: an identical copy on a later export date, or an older copy on an earlier export date. The formula for `p` is explained at the top of `batch_exports.py`.

**3. A Sale Event changes core parameters, not the data after generation.** During the two Prime Day days, the core multiplies the Order count of the Amazon Storefronts, raises the share and depth of discounts, and concentrates demand on a group of deal SKUs. The discount level is already part of the return-probability function, so the return rate on those days rises on its own without separate tuning. That is covariate drift: the features change distribution, while the relationship between features and label stays the same.

**4. Native Channel IDs are derived from canonical IDs by hashing.** `AmazonOrderId`, `purchaseOrderId`, ASIN and tracking numbers are not drawn at random but hashed from the canonical ID (`ORD-017-00042`) together with the seed. The same Order always has the same native ID in every export, in both Batch and Stream, and the renderer needs no lookup table.

### Measured on the full run (Batch)

| Metric | Target | Measured |
|---|---|---|
| Duplicate rate `amazon.orders` | 2% | 1.98% (4,819 older copies, 4,933 identical copies) |
| Duplicate rate `walmart.orders` | 2% | 1.98% |
| Overall return rate | about 10 to 12% | 11.2% |
| Apparel return rate | about 22% | 23.3% |
| Home goods return rate | about 4% | 4.6% |
| Amazon Orders per day during the Sale Event | 4 times a normal day | 10,642 vs 2,679, i.e. 3.97 times |
| Average Amazon discount during the Sale Event | about 21% | 21.0% vs 4.4% on normal days |
| Amazon return rate during the Sale Event | higher than normal days | 13.5% vs 11.3% |
| Best-selling SKU among Amazon Order Lines | about 5% | 5.4% (measured on the 60-day run) |

The return rates match the targets because every effect in the logistic function is centered (mean subtracted), as explained at the top of `returns.py`.

### Stream

The `generator.publish_order_snapshot_events` command regenerates about the last 20 days before `cutoff` (to catch old Orders that still change status), then continues for `days_stream` days. Each status change of an Amazon or TikTok Shop Order within `[cutoff, stream_end)` becomes one message:

| Topic | Shape | Dedup key | Problem |
|---|---|---|---|
| `amazon.order-notifications` | `ORDER_CHANGE` notification (assumed, not yet checked against official documentation) | `NotificationMetadata.NotificationId` | Late: `EventTime` is the status change time, `PublishTime` is the arrival time |
| `tiktok_shop.order-webhooks` | Webhook type 1, per channel-sources.md section 5 | `tts_notification_id` | Duplicate, burst |

The Kafka key is the native order ID, so the messages of one Order go to the same partition. Messages are sent in arrival order, so a late Amazon message comes after messages with a later `event_time`.

**The burst lives in the core, not in the delivery layer.** Like Prime Day, the livestream changes core parameters: for a few hours, the TikTok Shop Order count is multiplied by `live_multiplier`. The multiplier is drawn once within the `burst_multiplier` range and recorded in `stream_manifest.json`. The extra Orders use their own random number generator, so the other Orders of that day are unchanged.

Measured on the `configs/default.yaml` run into Kafka through Compose (`STREAM_SPEED=60`, 43.7 seconds). The message count in Kafka matches the count. Full report with how to verify: [data-quality-report-stream.md](data_quality_report_stream.md).

| Metric | Target | Measured |
|---|---|---|
| TikTok Shop messages | | 93,554 |
| Amazon messages | | 399,410 |
| TikTok Shop duplicate rate | 1.5% | 1.49% |
| Amazon duplicate rate | 0 | 0 |
| Amazon median delay | a few minutes | 5.3 minutes |
| Amazon messages delayed more than 1 hour | 5% | 4.98% |
| Amazon messages arriving after a message whose `event_time` is more than 1 hour later | yes | 19,729 |
| New TikTok Shop Orders per hour during the livestream vs the same hour on other days | 5 to 8 times (answer key 5.45) | 148.5 vs 25.2, i.e. 5.89 times |

`measured_ratio` compares against the average of the other Stream days, so it is affected by whether the livestream day happens to be busy or quiet. Compared with the same day with the livestream turned off, the ratio is 5.09. See [data-quality-report-stream.md](data_quality_report_stream.md#tiktok-livestream-burst).

### Simplifications compared with real Channels

- Real Amazon v0 splits Order Items into a separate `getOrderItems` call. Here `OrderItems` sit in the same record, one JSON line per Order.
- All Order Lines of an Order share the same status; there is no partial delivery.
- Each Order Line's Return is decided independently, with no correlation between lines of the same Order. In the real world, a buyer who returns one item is more likely to return other items in the same order (buying several sizes to try, being unhappy with the whole order). So the data does not yet have a "whole order returned together" signal for the model to learn.
- Walmart writes to `pg-load/walmart.orders/`, the rows that the `load-postgres` command loads into the `walmart.orders` table.
- Exchange rates are temporarily fixed in the Storefront's `price_factor`. The daily exchange-rate table is done in step 2b.
- Amazon notifications in the Stream always use v0-style status values (`Shipped`), not changing with the schema switch date.
- The livestream has no concept drift yet (higher apparel return rate). This part is done in step 4.


### Configuration keys in use

The generator reads YAML into dataclasses in `generator/config.py`. Keys absent from the file take their default value. A mistyped key stops the command with an error instead of being ignored. Two sample files: `configs/small.yaml` (a test run of a few seconds) and `configs/default.yaml` (full scale).

| Key | Default | Meaning | Related data problem |
|---|---|---|---|
| `seed` | 42 | Random seed. The same seed and configuration give the same data | |
| `scale.n_skus` | 2000 | Number of SKUs in the Seller's catalog | Cardinality of SKU, ASIN |
| `scale.start_date` | 2026-01-01 | Simulated day 1 | |
| `scale.days_history` | 180 | Number of history days. `cutoff` is 00:00 UTC right after the last day | Volume |
| `scale.days_stream` | 30 | Number of Stream days after `cutoff` | |
| `scale.orders_per_day` | 4000 | Orders per day across all 7 Storefronts, before the Sale Event multiplier | Volume, cardinality |
| `scale.sku_zipf_exponent` | 0.8 | Skew of the SKU distribution. Larger means the best-selling SKU takes a bigger share | Skew by SKU |
| `storefront_weights.<storefront>` | Amazon US 0.45, ... | Order share by Storefront, must sum to 1 | Skew by Storefront |
| `labels.window_days` | 30 | Return Label window counted from delivery date | |
| `offline_problems.duplicate_rate` | 0.02 | Per-key duplicate rate in each dataset, within [0, 0.5) | Duplicate |
| `offline_problems.overlap_days` | 3 | How many recent days an export re-pulls. Duplicates fall in this range | Duplicate |
| `offline_problems.amazon_orders_api_switch_day` | 100 | From this export day on, Amazon writes Orders 2026-01-01 | Schema evolution |
| `drift.prime_day.day`, `.days` | 120, 2 | Start day and length of the Sale Event on Amazon Storefronts | Skew by day, covariate drift |
| `drift.prime_day.order_multiplier` | 4 | How many times a normal day's Amazon Order count on sale days | Skew by day |
| `drift.prime_day.discount_share` | 0.7 | Share of Amazon Order Lines discounted on sale days | Covariate drift |
| `drift.prime_day.deal_skus` | 50 | Number of SKUs that attract most demand on sale days | Skew by SKU |
| `streaming_problems.pacing.default_seconds_per_sim_day` | 60 | Real seconds per simulated day when emitting the Stream | Burst visible in the Flink UI |
| `streaming_problems.pacing.segments` | empty | List of `{from_day, to_day, seconds_per_sim_day}` emitted at their own speed | Burst visible in the Flink UI |
| `streaming_problems.duplicate_rate.<storefront>` | `tiktok_shop_us: 0.015` | Duplicate rate by dedup key in the Stream | Duplicate |
| `streaming_problems.amazon_late.median_minutes` | 5 | Median delay of the notifications delayed under 1 hour | Late |
| `streaming_problems.amazon_late.share_over_1h` | 0.05 | Share of Amazon notifications delayed 1 to 6 hours | Late, out of order |
| `drift.tiktok_live.day` | none | Simulated day with a livestream, must be within the Stream range | Burst |
| `drift.tiktok_live.start_hour_utc`, `.duration_hours` | 12, 6 | Start hour and length of the live session | Burst |
| `drift.tiktok_live.burst_multiplier` | [5, 8] | Range of the multiplier on TikTok Shop Orders during the live session | Burst |
| `storage.output_root` | `./out` | Where to write: a local directory or `s3://bucket`. The `batch --output` command overrides this key | |

Concept drift for the livestream (higher apparel return rate) is added in step 4.

### Run results

The numbers below come from the `configs/default.yaml` run through Docker Compose: `generator-batch` writes to MinIO (102 seconds), `generator-load` loads PostgreSQL (3 seconds). Then `python -m generator report --source s3://platform --dsn ... --markdown` runs on the host, reading Amazon from MinIO and Walmart and `seller.skus` from PostgreSQL. The `_truth` columns come from `run_manifest.json`.

Screenshots (the `report` command terminal, MinIO console, DBeaver or `psql`) are in `docs/images/batch/` and embedded in [data-quality-report-batch.md](data_quality_report_batch.md).

How to read each table:

- **storefront_skew**: Order counts measured from the exported data match the answer key exactly for all three Amazon Storefronts and Walmart. The three Storefronts without a renderer yet are left blank.
- **city_skew**: the largest city accounts for about 46 to 49% of each Storefront's Orders.
- **category_skew**: category shares are skewed (apparel 17.7%, beauty 7.6%), and return rates differ by category (apparel 23%, home 4.6%). Category only exists in `seller.skus`, so this table has to join with PostgreSQL.
- **top_skus**: the best-selling SKU accounts for 5.4% of Order Lines. When Spark groups by SKU, the task holding this SKU will be noticeably slow.
- **daily_orders**: the two Sale Event days have about 3.7 times the median Order volume.
- **cardinality**: about 500 thousand Amazon Order IDs and 720 thousand Order Line IDs. DuckDB's HyperLogLog is off by more than 20% on some columns, so the table also prints exact counts. The later Spark job will be compared against these exact counts.
- **schema_evolution**: `proceeds` is 100% null before 2026-04-10 and 0% from that date on. `IsPrime` is the reverse. The boundary falls exactly on one export date.
- **duplicates**: the measured duplicate rate equals the answer key exactly (1.98% and 1.99%, target 2%), and is 0 after keeping the latest copy per key. In Amazon, about half of the duplicates are older copies (old status), so dedup that only drops identical copies is not enough.
- **volume**: Amazon 388 MB of JSON Lines in 180 files, one file per export date. The `walmart.orders` table in PostgreSQL is 74 MB.

#### Table `storefront_skew`

Orders by Storefront. An empty `orders_exported` means the Channel has no renderer yet.

| storefront_id | config_weight | orders_truth | pct_truth | orders_exported |
|---|---|---|---|---|
| amazon_us | 0.45 | 334046 | 45.38 | 334046 |
| tiktok_shop_us | 0.15 | 108042 | 14.68 |  |
| amazon_uk | 0.12 | 89332 | 12.14 | 89332 |
| amazon_de | 0.1 | 74778 | 10.16 | 74778 |
| shopify | 0.08 | 57673 | 7.83 |  |
| walmart_us | 0.07 | 50499 | 6.86 | 50499 |
| shopee_vn | 0.03 | 21759 | 2.96 |  |

#### Table `city_skew`

A few large cities account for most of each Storefront's Orders.

| storefront_id | city | orders | pct |
|---|---|---|---|
| amazon_de | Berlin | 36723 | 49.11 |
| amazon_de | Hamburg | 15019 | 20.08 |
| amazon_de | Munich | 8836 | 11.82 |
| amazon_de | Cologne | 6112 | 8.17 |
| amazon_de | Frankfurt | 4404 | 5.89 |
| amazon_de | Stuttgart | 3684 | 4.93 |
| amazon_uk | London | 43798 | 49.03 |
| amazon_uk | Manchester | 17889 | 20.03 |
| amazon_uk | Birmingham | 10376 | 11.62 |
| amazon_uk | Leeds | 7406 | 8.29 |
| amazon_uk | Glasgow | 5668 | 6.34 |
| amazon_uk | Bristol | 4195 | 4.7 |
| amazon_us | New York | 154019 | 46.11 |
| amazon_us | Los Angeles | 62144 | 18.6 |
| amazon_us | Chicago | 36338 | 10.88 |
| amazon_us | Houston | 25409 | 7.61 |
| amazon_us | Phoenix | 18969 | 5.68 |
| amazon_us | Seattle | 14825 | 4.44 |
| amazon_us | Miami | 12121 | 3.63 |
| amazon_us | Denver | 10221 | 3.06 |
| walmart_us | Dallas | 24843 | 49.2 |
| walmart_us | Houston | 9851 | 19.51 |
| walmart_us | Atlanta | 6055 | 11.99 |
| walmart_us | Chicago | 4142 | 8.2 |
| walmart_us | Phoenix | 3112 | 6.16 |
| walmart_us | Orlando | 2496 | 4.94 |

#### Table `category_skew`

Order Lines by category. `return_pct_truth` is the true return rate from the manifest.

| category | order_lines | pct | return_pct_truth |
|---|---|---|---|
| apparel | 140711 | 17.71 | 23.32 |
| toys | 115003 | 14.48 | 6.69 |
| electronics | 112797 | 14.2 | 8.1 |
| accessories | 105104 | 13.23 | 10.16 |
| sports | 90499 | 11.39 | 8.3 |
| home | 85100 | 10.71 | 4.57 |
| footwear | 84430 | 10.63 | 16.08 |
| beauty | 60717 | 7.64 | 5.83 |

#### Table `top_skus`

The best-selling SKU should account for about 5% of Order Lines: this creates a slow task for Spark.

| sku | order_lines | pct |
|---|---|---|
| ELE-01103 | 43073 | 5.42 |
| TOY-01453 | 24485 | 3.08 |
| SPT-01304 | 18065 | 2.27 |
| ACC-00934 | 14265 | 1.8 |
| TOY-01474 | 11904 | 1.5 |
| FTW-00683 | 10132 | 1.28 |
| ACC-00929 | 9094 | 1.14 |
| TOY-01568 | 8137 | 1.02 |
| HOM-01853 | 7522 | 0.95 |
| SPT-01238 | 7264 | 0.91 |

#### Table `daily_orders`

Days with the most Orders (Amazon and Walmart). Sale Event days in the manifest: ['2026-04-30', '2026-05-01'].

| day | orders | x_median |
|---|---|---|
| 2026-05-01 | 10950 | 3.7 |
| 2026-04-30 | 10887 | 3.68 |
| 2026-03-13 | 3119 | 1.05 |
| 2026-03-07 | 3079 | 1.04 |
| 2026-04-29 | 3077 | 1.04 |

#### Table `cardinality`

Number of distinct values: DuckDB's HyperLogLog estimate and the exact count. DuckDB's HyperLogLog uses few counters, so it can be off by more than 10%.

| dataset | column | approx_distinct | exact_distinct | approx_err_pct | rows |
|---|---|---|---|---|---|
| amazon.orders | order_id | 385608 | 498156 | -22.59 | 508227 |
| amazon.orders | line_id | 818537 | 721099 | 13.51 | 721099 |
| amazon.orders | asin | 1844 | 2000 | -7.8 | 721099 |
| amazon.orders | sku | 1526 | 2000 | -23.7 | 721099 |
| walmart.orders | order_id | 50400 | 50499 | -0.2 | 51523 |
| walmart.orders | buyerId | 21549 | 21719 | -0.78 | 51523 |
| walmart.orders | line_id | 68348 | 73262 | -6.71 | 73262 |
| walmart.orders | tracking_number | 52956 | 48629 | 8.9 | 73262 |

#### Table `schema_evolution`

amazon.orders changes schema from export_date 2026-04-10: the `proceeds` column is null in old partitions and the v0-only columns are null in new partitions — because key names changed, not because there are new fields.

| period | export_dates | rows | pct_null_proceeds | pct_null_IsPrime | pct_null_AmazonOrderId |
|---|---|---|---|---|---|
| before (Orders v0) | 99 | 247213 | 100.0 | 0.0 | 0.0 |
| after (Orders 2026) | 81 | 261014 | 0.0 | 100.0 | 100.0 |

#### Table `schema_evolution_daily`

Null rate per export_date around the schema change date 2026-04-10.

| export_date | rows | pct_null_proceeds | pct_null_IsPrime |
|---|---|---|---|
| 2026-04-07 | 2786 | 100.0 | 0.0 |
| 2026-04-08 | 2632 | 100.0 | 0.0 |
| 2026-04-09 | 2819 | 100.0 | 0.0 |
| 2026-04-10 | 2688 | 0.0 | 100.0 |
| 2026-04-11 | 2676 | 0.0 | 100.0 |
| 2026-04-12 | 2707 | 0.0 | 100.0 |

#### Table `duplicates`

Duplicate rate = 1 - count(distinct key) / count(*). Dedup keeps the latest update per key. Walmart: each pull has its own `fetched_at`, so every duplicate becomes an older copy.

| dataset | rows_before | distinct_keys | dup_pct_before | identical_copies | older_versions | rows_after | dup_pct_after | dup_pct_truth |
|---|---|---|---|---|---|---|---|---|
| amazon.orders | 508227 | 498156 | 1.98 | 5108 | 4963 | 498156 | 0.0 | 1.98 |
| walmart.orders | 51523 | 50499 | 1.99 | 0 | 1024 | 50499 | 0.0 | 1.99 |

#### Table `volume`

Size after storage, by storage location and format.

The `files` numbers below were measured before each simulated day had its own file; now each
`export_date=` directory has many part files, so the file count is roughly an order of magnitude larger, while `size_mb`
is nearly unchanged. Re-run `uv run python -m generator report` to get the new numbers.

| dataset | storage | format | files | size_mb |
|---|---|---|---|---|
| amazon.orders | object store | JSON Lines | 180 | 388.02 |
| walmart.orders | PostgreSQL (staging) | JSON Lines | 180 | 59.66 |
| seller.skus | PostgreSQL (staging) | JSON Lines | 1 | 0.22 |
| labels | object store | Parquet | 1 | 13.26 |
| order_line_keys | object store | Parquet | 2 | 21.72 |
| walmart.orders | PostgreSQL (table, including indexes) | heap, JSONB column |  | 73.61 |
| seller.skus | PostgreSQL (table, including indexes) | heap |  | 0.31 |

## On AWS

The generator runs on EC2, the batch is stored in S3/PostgreSQL, and the stream is emitted to Kafka on Compose.
The [batch receipt](../../evidence/aws/data-platform-aws-20261027.json) records 141,774,479,087 bytes of raw S3;
the [stream manifest](../../evidence/aws/stream-manifest-20261028.json) records 399,410 Amazon and 93,554 TikTok messages.

![DataHub filtered to AWS S3: four source datasets Amazon, Shopee, Shopify and TikTok Shop under source-exports](../../evidence/aws/datahub/s3-sources.png)

![Flink dedup_tiktok_state_proof: HASH edge to Deduplicate, source sent 93,554 records, job FINISHED](../../evidence/aws/flink-dedup-graph-20261028.png)

The S3 source metadata screenshot and Flink receiving records confirm parts of the data path;
AWS characteristic numbers and limitations are recorded in [batch on AWS](data_quality_report_batch.md#on-aws)
and [stream on AWS](data_quality_report_stream.md#on-aws).
[Generator code](../../../generator/__main__.py), [batch config](../../../configs/scale-100gb.yaml), [stream config](../../../configs/default.yaml).
There is no separate AWS report on batch skew/cardinality/schema/duplicates yet; local histograms are not used in place of a cloud capture.
