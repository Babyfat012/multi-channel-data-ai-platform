# Silver layer design — cleaning and normalizing to a common language

This document records the **settled design** of the Silver layer: two per-Channel `stg_*` tables, Order Line
grain, dedup keys, data types, and how differences between Amazon's two schema shapes
are handled. 

Every number here comes from a line of code (written as `file.py:line`) or is explicitly marked **"measured"** / **"not measured"**.
Where something is not measured, there is no guessing.

---

## 1. Scope of this document

- The settled design, and the **existing code**: `spark_jobs/silver/build_amazon_order_snapshots.py`,
  `spark_jobs/silver/build_walmart_order_snapshots.py`. The mapping tables are split out into `spark_jobs/lib/order_snapshot_mapping.py`
  (it does not import pyspark, so it can be tested on the host). Section 10 records the real run measurements.
- The document describes *decisions, reasons, and costs* — it is not a how-to-run guide.
- Section 8 records **the last three settled items**. Section 9 records the Gold decision implemented as a consequence.
- Section 10 records **the real run measurements** and **two new items that came up while running**.
- Read together with: [terminology and data layers](../../shared/terminology.md) (Bronze/Silver/Gold definitions), `docs/data-platform/reference/spark.md` (Bronze, done).

---

## 2. Two separate per-Channel tables, NOT merged

**Settled:** two separate Silver tables — `stg_amazon_order_lines`, `stg_walmart_order_lines`.
The `stg_*` table names are implemented; tables run through Airflow have the `_dp2` suffix.

| | Amazon | Walmart |
|---|---|---|
| Dedup key shape | 2 columns | 3 columns |

**Reason (raised by the user):** the two Channels have different dedup key shapes. Merging would mean one dedup job has to
branch on `channel` — awkward in code, and adding a new Channel would mean editing that same job again.

**The `channel` column is kept in BOTH tables**, so the later merge step does not have to guess where a row came from.

**The place where the two Channels are merged is Gold** (`fact_`/`obt_`), not Silver. Basis: [terminology and data layers](../../shared/terminology.md)
defines Silver as "merging the schema shapes of **the same** Channel" — merging multiple Channels does not
belong to Silver.

**Cost:** splitting does not remove the merge, it only pushes it down to Gold, where both Channels are already snake_case
and clean.

**Aligning the Bronze definition:** the Bronze definition in `docs/data-platform/reference/spark.md:17` and [terminology and data layers](../../shared/terminology.md) has been
corrected to match the real design — Bronze has **one table per CHANNEL**, and Amazon's two schema shapes share
it because Bronze does not parse JSON.

---

## 3. Grain and keys

**Grain = Order Line.** One Bronze row is one Order containing an array of Order Lines, so Silver has to
explode the array. Reason given by the user: "the finer the grain, the closer to prediction" — the Return
Label is scored at the Order Line level.

**Column names:** snake_case for **every** column, consistent with Bronze naming (Bronze columns
are all snake_case: `payload`, `source_file`, `export_date`, `ingested_at`, `run_id`,
`raw_payload`, `fetched_at`, `source_table`).

### 3.1 Dedup keys

| Channel | Key | Tie-break |
|---|---|---|
| Amazon | `(order_id, order_item_id)` | keep the largest `last_updated_time` |
| Walmart | `(purchase_order_id, customer_order_id, line_number)` | latest `status_changed_at` |

**Settled (when the tie-break ties — provenance):** keep the record with the **largest** `export_date` (Walmart:
largest `fetched_at`).

**Reason:** the user chose "keep the latest record" for the main tie-break (largest `last_updated_time`).
Using the same direction means the whole dedup rule takes only **one** sentence to explain: always take
the latest.

**Cost:** the information "on which day was this order first seen" is lost — Bronze keeps it all and it can be looked up again.

**Settled (Walmart key names):** Silver **keeps the source names** (`purchase_order_id`, `line_number`),
and does **not** add an `order_id` column to the Walmart table. **Renaming happens in Gold**, not Silver.

**Reason:** `purchase_order_id` is **not as reliable** as Amazon's `order_id`. The Walmart ID is
`"1"` + 12 hashed digits (a space of 10^12), and **a real collision has been measured**: the value
`1678172477013` appears at `export_date` 2026-01-20 and 2026-02-17 as **two different orders**
(Houston vs Orlando, `customerOrderId` 9698229271973 vs 9655369243011). Naming both columns
`order_id` would make that difference **disappear** from the table — readers would think the two columns are equally reliable.
Keeping the source name keeps the warning.

**Cost:** Gold has to carry an extra rename step; in exchange, there are no two columns of different nature sharing
one name.

**Why `order_id` alone is WRONG:** at the Order Line level, one `order_id` spans several real
rows, so deduplicating on `order_id` alone would **delete real Order Lines**. This is a **silent** bug — the job
reports nothing, and the row count still looks plausible.

### 3.2 Step order: parse FIRST, dedup AFTER

**Measured:** 23,487 keys have one row in v0 shape and one row in 2026 shape (`export_date` window
2026-04-07..04-12). Until key names are normalized into one shape, dedup cannot recognize them as the same order.

**Dedup cannot be done within each partition:** duplicates always sit on a different export day
(`overlap_days = 3`), so dedup within one partition catches 0 duplicates. It has to look across
≥ ±3 `export_date` values.

---

## 4. Data types

### 4.1 Money = `decimal(14,2)`, both Channels

**Real bounds:**
- max base price 300 USD (`generator/core/storefronts.py:99`, electronics category `(20, 300)`)
- max quantity 4 (`generator/core/simulate.py:302`)
- Amazon/Walmart price factors 1.00 / 0.80 / 0.95

=> one row ≤ ~1,200. `decimal(14,2)` is more than enough.

**Why NOT `double`:** rounding errors, and the errors **accumulate** when summing 108 million rows. Gold will
sum revenue per day and per SKU => two different summation orders can give two different numbers.

**Why NOT `string`:** it only **postpones** the decision to the casting step; it does not solve it.

**Why both Channels MUST use the same type:** Gold unions the two tables; a union of `decimal` with `double` makes Spark
promote both to `double`, breaking the Amazon part that was correct.

**Cost:** VND has no fractional part. If the Shopee Channel later uses the same type, every VND row
carries a meaningless ".00". That is the cost of using **one** type for every Channel. (Shopee bound:
300 × factor 17,500 × 4 = 21,000,000 — `storefronts.py:55`.)

**Iceberg trap:** `p` can be widened (14 → 18), but `s` can **not** be changed. So `s = 2` is a one-time choice.

**Settled (names of the three row-level money columns):** `line_gross_amount` (before discount),
`line_discount_amount`, `line_net_amount` (after discount). **Approved.** Amazon has all three; the Walmart
source only provides **net**, so the Walmart table has **only** `line_net_amount`, without creating two empty columns.
Only Amazon has `order_total_amount`.

**Reason:** the line price **means different things** in the two Channels — Amazon `ItemPrice` is **before** discount
(`generator/channels/amazon.py:157`, discount separate at `:158`); Walmart `chargeAmount` is
**after** discount (`generator/channels/walmart.py:103`). They must not share one name,
otherwise Gold would sum two different things into one place.

**Cost:** three different names force query authors to know which one they are summing; but that is exactly
what the **separate tables** option buys — no empty columns created for a Channel that does not have them.

### 4.2 Time = UTC `timestamp`, and PIN the time zone

Amazon stores ISO strings with `Z`; Walmart stores epoch **milliseconds**. Silver converts both to UTC
`timestamp`. Reason given by the user: keeping both forms makes it hard to read — looking at `1678172477013` you cannot tell
what day it is.

**Pin `spark.sql.session.timeZone = UTC`** in the Silver job's SparkSession configuration
(**not** scattered across individual conversions).

**Limitation that must be stated:** converting to `timestamp` does **not** remove time zone risk, it only concentrates the conversion in
**one** place. Spark's date function (`to_date`) changes its result according to `spark.sql.session.timeZone`, and
the Return Label target has a **30-day window** — the day boundary decides whether a row gets labeled
or not. Without pinning, the same data on a machine in a different time zone yields different labels.

**Consistency with Bronze:** Bronze already has real time-typed columns — `ingested_at`
(`spark_jobs/bronze/ingest_amazon_order_snapshots.py:194`), `fetched_at`, and `raw_walmart_orders` is partitioned by
`days(fetched_at)`.

### 4.3 `is_prime` as `boolean`

v0 has `IsPrime` (bool); the 2026 shape has `programs` (array). Silver uses **one** `is_prime` column
of type `boolean`; the 2026 shape is parsed by checking whether the array contains `PRIME`.

**Decisive fact:** `generator/channels/amazon.py:189`
`"programs": ["PRIME"] if order.is_prime else []` — the array has only **two** possibilities, exactly one
bool, no more than one bit of information.

**Additional reason:** the target is a feature table; a model takes a `boolean` directly, while an `array<string>`
would still have to be expanded into a 0/1 flag in Gold => just postponing the work.

**Counterargument considered:** "the source may add other programs later" => at that point **add a column** (Iceberg allows
adding columns cheaply). Choosing an array now pays a certain cost today to guard against something that may never
happen.

### 4.4 `country`: cast Walmart `"USA"` to `"US"`

**Settled:** the `country` column uses one common value set; Walmart `"USA"` is cast to `"US"`.

**Reason:** `US` is the 2-letter ISO 3166-1 alpha-2 code, and Amazon already uses it. This does **not** contradict
section 6 (raw status is kept verbatim): status has **two** columns (raw + canonical), so the raw column has
room to stay faithful; `country` has only **one** column, so that column must be the canonical one.

**Cost:** the original spelling `"USA"` is lost in Silver — Bronze still keeps `"USA"` verbatim and it can be looked up.

---

## 5. Five keys dropped in Amazon's 2026 shape: keep 3, drop 2

**Dropped from Silver entirely:** `is_business_order`, `is_replacement_order`. In the v0 shape they are constant
`False` (`generator/channels/amazon.py:142-143`) => every row is the same, there is nothing for a model
to learn (zero variance). Bronze still keeps the raw payload, so nothing is lost permanently.

**Kept:** `number_of_items_shipped`, `number_of_items_unshipped` (Order level), `quantity_shipped`
(Order Line level). These three columns say "how many items have shipped / how many have not"; an unshipped order is
not delivered, and an undelivered order cannot be returned => directly relevant to the Return Label target.

**For the 2026 shape (payload lacks the keys): leave NULL.** No backfilling from status + quantity.
Reason given by the user: "to keep the data as faithful as possible, like a real training setup".

**The two costs of backfilling (if it were done):**
1. the derived number is only correct **as of that export day**, and after dedup keeps the latest record the column means
   "as of the latest record", not the final total;
2. the column looks **like** source data but is self-computed, and it is derived **from** status =>
   a model learning on it is just relearning status, circularly.

**Null rate: NOT MEASURED, needs the Spark cluster.** A firm lower bound is ≥ **6.95%** of rows
(7,524,691 / 108,248,563). The 48% figure by file bytes is **an indicator**, **not** a measurement.

---

## 6. Status: TWO columns

[terminology and data layers](../../shared/terminology.md) has already settled this: "the Channel's native status is kept separately and mapped to the canonical
status". So Silver has two columns — this is **not** a project choice.

- **Canonical status** `order_status`, exactly the list in [terminology and data layers](../../shared/terminology.md): `pending_payment` →
  `awaiting_shipment` → `shipped` → `delivered` → `completed`, plus the two branches `cancelled` and
  `returned`.
- **Raw status column** `channel_status`: keeps the source text verbatim. `Canceled` (v0) and
  `CANCELLED` (2026) are left as is, not normalized.

**Reason:** the raw column exists **only** for faithfulness. Editing its text takes away the very reason it
exists.

**Cost that must be stated:** the raw column has **5 values for 4 meanings**. Anyone filtering on the raw column using only one
spelling will **undercount**, with no error. => **Do not filter on the raw column; use `order_status`.**

**A consequence of the source, recorded for completeness:** Amazon **never** emits `delivered`/`completed` (a
delivered order is still `Shipped`). Walmart has a real `Delivered` with `statusDate`, **but** `COMPLETED` is also
written as `Delivered` (`generator/channels/walmart.py:38-39`) — so for completed orders,
`statusDate` is the **completion** time, not the delivery time. That is why the Silver column is named
`status_changed_at`, **not** `delivered_at` — see section 8.2.

---

## 7. Provenance carried into Silver

**Settled:** Amazon carries `export_date` + `source_file`; Walmart carries `source_table` + `fetched_at`.
Bronze's `ingested_at` and `run_id` are **not carried** — Silver writes its own columns.

**Reason:**
- `export_date` must be in Silver — it is the tie-break column settled in section 3.1 (when
  `last_updated_time` ties, keep the largest `export_date`). Without carrying it, that tie-break cannot be
  performed.
- `source_file` lets a Silver row be traced back to the exact Bronze file that produced it.
- `ingested_at` and `run_id` describe "when **Bronze** wrote" and "which **Bronze** run". Carried into
  Silver, those two columns would describe a different layer and could easily be read as Silver's write time. Silver writes its
  own columns.

**Cost:** from Silver alone you cannot tell which Bronze run produced a row. But
`source_file` + `export_date` already narrow it down enough, and Bronze still keeps `run_id`.

---

## 8. The last three items: DECIDED

These three items used to be in the "undecided" section. They are now settled, with reasons and costs.

### 8.1 Four Walmart-only columns: keep 2, drop 2

**Settled (chosen by the user):** keep `buyer_id`, `is_guest`. Drop `customer_email_id`, `order_type`.

**Reasons for dropping:**
- `order_type` is **always `"REGULAR"`** — hardcoded in `generator/channels/walmart.py:119`, and the whole repo
  has no second value. A single-valued column carries no information.
- `customer_email_id` is a hashed email (`walmart.py:114`), duplicating the *identity* role of `buyer_id`
  while adding one more personal-data column to guard.

**Cost of dropping: NO data loss.** Bronze keeps `raw_payload` verbatim, and those two keys are still
there. To use them again, edit the Silver job and rerun from Bronze — it costs **effort**, not **data**.

**Cost of KEEPING — the easy-to-miss part:**
- `is_guest` **carries no signal about the Return Label in this data**: `generator/core/returns.py`
  does not read `buyer` (grep: not a single line). Put it into the feature table and a model may still pick up
  a spurious correlation — a column that *looks* useful but is actually noise. This must be stated clearly at the
  feature layer, so later readers do not assume it is a vetted feature.
- `buyer_id` is kept as a **join key**, **not** as a feature (an operator recommendation, with no code yet to
  enforce it). It is a 12-digit hashed ID (`native_code(..., 12)`, a space of 10^12) — the grain is almost
  one-row-one-value. Using it directly as a feature leaks identity rather than learning a pattern.

### 8.2 Walmart's delivery time: the column is named `status_changed_at`, NOT `delivered_at`

**Settled:** `stg_walmart_order_lines` has a `status_changed_at` column (UTC timestamp) taken from `statusDate`.
Silver does **not** create a `delivered_at` column.

**Reason:** `generator/channels/walmart.py:34-39` maps **both `DELIVERED` and `COMPLETED`** to
the same text `"Delivered"`, and `statusDate` is the time of the **most recent status change**
(`walmart.py:77,107` — `epoch_millis(change.at)`). So for a completed order, `statusDate` is
the **completion** time, not the **delivery** time, and the payload **cannot distinguish the two cases**.
[terminology and data layers](../../shared/terminology.md) defines **Delivered At** as "the time the Order Line was actually delivered" and as
**the start of the 30-day window** of the Return Label. A column with that name holding the completion time
would skew the label **silently** — nobody sees an error, only wrong labels. So the Silver column
is named for what it actually is.

**Cost:** Silver has no reliable `delivered_at`; the Gold fact does not infer this column either. Labels and delivery time live only in a separate bridge (section 9). Also note a known consequence of the dedup
key settled in section 3.1 (keep the record with the latest `last_updated_time`): it keeps the **completion** time
and drops the **delivery** time. So Gold cannot build `delivered_at` from Silver alone — see section 9.

### 8.3 Name and level of Fulfillment Method

**Settled:** the column name is `fulfillment_method`, at the **Order level**, repeated on every Order Line of the same
Order. Only `stg_amazon_order_lines` has this column; `stg_walmart_order_lines` **does not**.

**Reason for the name:** [terminology and data layers](../../shared/terminology.md) chose **Fulfillment Method** as project vocabulary, with
`_Avoid_: Shipping type, logistics mode`. `fulfilled_by` is not used because it is the **raw key name** in
Amazon's 2026 shape (`generator/channels/amazon.py:178`) — using a raw name for a canonical column would mix two
layers, exactly the mistake section 6 avoided by splitting the two status columns.

**Reason for the level:** [terminology and data layers](../../shared/terminology.md) defines it per Order Line, but the data **does not exist at that level**:
`generator/core/model.py:102` puts `fulfillment` on the `Order` class (`model.py:88`), while the
`OrderLine` class (`model.py:60`) has no such field. Amazon publishes it at the Order level in both shapes
(`amazon.py:135` `FulfillmentChannel`, `amazon.py:178` `fulfillment.fulfilledBy`). Walmart:
grepping `fulfill` in `generator/channels/walmart.py` returns **nothing** — this Channel publishes nothing.

**Cost:**
- The column sits at Order Line grain but its value is only correct at the Order level, so it **cannot describe** an
  Order with two Order Lines fulfilled by different parties. The generator currently **does not produce** that case, so nothing is lost
  today; if it does later, both this column and the code reading it must change.
- `stg_walmart_order_lines` lacks the column, so a Gold table merging both Channels must accept **null for
  Walmart** — and null here means "the Channel does not publish it", not "unknown".

---

## 9. Consequences carried into Gold — settled

Silver keeps the latest snapshot, so Walmart's `statusDate` cannot prove
the real delivery time. The renderer projects both `DELIVERED` and `COMPLETED` onto
the source text `"Delivered"`; a delivered status may never have been observed
in the Bronze exports. The exact delivery time cannot be inferred from the first snapshot
that shows that text.

Implemented decision: the Gold fact has **no `delivered_at`**; DP3 features
use only information known at the time the Order was placed. Walmart labels, together with delivery time
and label availability time, are joined from ground truth in a separate table, via the full
`purchase_order_id`, `customer_order_id`, `line_number` to avoid ID collisions.
The bridge recorded 1,319,345 labeled Order Lines, with no errors/duplicate keys in that run.
See the [Gold decision](gold_design.md) and the
[Gold/DP3 evidence](gold.md). No inferred delivery column needs to be added to Silver.

---

## 10. Real run measurements

Run on two existing SMALL Bronze tables, without touching the real tables. The two test Silver tables were
`DROP ... PURGE`d after measuring.

### 10.1 Walmart — `raw_walmart_orders_smoke` -> 1,875,607 Order Lines

| | Number |
|---|---|
| Bronze rows (Orders) | 1,293,525 |
| Order Lines out | **1,875,607** |
| distinct dedup keys | **1,875,607** — no duplicate keys |
| duplicates removed | **0** (reason in 10.3, NOT a bug) |
| "DONE after" | 43.3s |

Three places where the data **agrees with itself**, used as cross-checks:
- `tracking_number` and `ship_date_time` are null on **129,438** rows, exactly the total number of unshipped orders
  (`Created` 619 + `Acknowledged` 78,100 + `Cancelled` 50,719 = 129,438). Matches to the row.
- `country` = `US` for **all** 1,875,607 rows => the `"USA"` -> `"US"` cast (section 4.4) works.
- `order_status` has **0 nulls** => every source status text is in the mapping table.
- Money: min 3.54 / max **1,139.64** — below the ~1,200 bound that section 4.1 computed BEFORE the code existed.
  `decimal(14,2)` is the right size, not a guess.

### 10.2 Amazon — merging two schema shapes, with numbers to prove it

`raw_amazon_orders_sample` (20 sample days) -> **18,307,929** Order Lines, "DONE after" 383.3s.

| | v0 shape | 2026 shape |
|---|---|---|
| Order Lines | 8,191,939 | 10,115,990 |
| raw status text | `Shipped` · `Canceled` · `Unshipped` · `Pending` | `SHIPPED` · `CANCELLED` · `UNSHIPPED` · `PENDING` |
| after normalization | `shipped` · `cancelled` · `awaiting_shipment` · `pending_payment` | **exactly those four values** |
| `fulfillment_method` | `AFN`/`MFN` -> `channel` 5,839,546 / `seller` 2,352,393 | `AMAZON`/`MERCHANT` -> `channel` 6,910,480 / `seller` 3,205,510 |
| `is_prime` | native bool | derived from the `programs` array, as bool |
| `number_of_items_shipped` | null **0%** | null **100%** |
| `number_of_items_unshipped` | null **0%** | null **100%** |
| `quantity_shipped` | null **0%** | null **100%** |

This is the evidence of schema evolution handling: two different spellings of
the same four meanings have converged into one set, and the three columns the 2026 shape lacks are **explicitly** null rather than
backfilled (section 5).

Other cross-checks: `126.54 − 22.12 = 104.42` equals that row's `line_net_amount` =>
`line_gross_amount − line_discount_amount` is computed correctly. Max line amount **1,199.64**,
max `order_total_amount` 2,125.07. 0 nulls in `order_id`, `order_item_id`, `created_at`,
`order_status`, `is_prime`, `line_net_amount`, `country`.

**Null rate of the three 2026-shape columns — now MEASURED, but only on the sample:** 10,115,990 / 18,307,929
= **55.3%** of the rows in the 20-day sample. Section 5 previously said "not measured, lower bound ≥ 6.95%". The two numbers
do NOT conflict: 6.95% is based on the row count of **all 180 days**, while 55.3% is for the **evenly spaced 20-day
sample**, and since the sample is spread evenly by DAY it picks up many late days (the last day is ~254×
heavier than the first). The true rate over 180 days is **still not measured** — it needs a job run on the real table.

### 10.3 Why the 20-day sample catches 0 duplicates

**Not a code bug.** The dedup key works correctly: 1,875,607 rows for 1,875,607 distinct keys.

The cause lies in **the source data of the two small Bronze tables**: their 20 `export_date` values are
**9-10 days** apart (2026-01-01, 01-10, 01-20, 01-29, 02-08, ...). The generator's duplicates always
fall within **±3 days** (`overlap_days = 3`, section 3.2). Days 9 days apart **cannot**
contain the same order twice. So a duplicate rate of 0 is **correct for this data**.

=> That is why Bronze must be loaded from **CONSECUTIVE** days. That **HAS BEEN DONE**, see section 10.6.

Consequence for tooling: `spark_jobs/lib/day_selection.py` now has **two** ways to pick days, and the two
are mutually exclusive because they serve opposite purposes:

| Function | Picks | To see |
|---|---|---|
| `pick_sample_days` | n **evenly spaced** days | **skew** — needs both small and large days in one run |
| `pick_window_days` | n **consecutive** days | **duplicates** — needs to cover the `±3` day window |

`spark_jobs/bronze/ingest_amazon_order_snapshots.py` has the flags `--window-days N` and `--window-start YYYY-MM-DD`; passing both
`--sample-days` and `--window-days` makes the job exit with code 2 rather than picking one itself.

### 10.4 SETTLED: partition column of both Silver tables = `days(created_at)`

Both jobs write `partitionedBy(days(created_at))` — partitioned by **order date**. This used to be
"open item no. 2"; **the operator has settled it**, keeping the column the code already uses, so the job
does not need changing.

Rationale: after dedup, `export_date` (Amazon) and `fetched_at` (Walmart) only mean
"of the winning record", no longer a complete slice of data — partitioning by them would make an order **jump
partitions** every time a new record arrives. `days(created_at)` is an Iceberg hidden partition: the column stays
`timestamp`, and queries filtering on an order date range read only the needed slice. Both tables use the **same**
partition column so that Gold's union of them does not have to read two different partitioning schemes.

**Cost of the chosen approach — expensive WRITES for cheap READS.** After dedup, orders updated in
one batch have `created_at` scattered across the whole 180-day range (an order placed in January still changes status today),
so one write touches **many scattered partitions**, not just the newest. Acceptable
because the Silver table is **written once a day** while the Gold layer **reads it many times**.

**Cost of the rejected approach (`export_date` / `fetched_at`) — lost READ time, and that is why it was dropped.**
These two columns change with each export, so an order **jumps partitions** every time a new record arrives: updating
one row has to rewrite **two** partitions (remove from the old one, add to the new one) instead of
one. Worse: the partition name only records `export_date` and **says nothing** about `created_at`, so
Gold queries filtering by order date **cannot prune partitions** (partition pruning) — a
partition `export_date=2026-04-07` can contain orders placed on any earlier day. Spark has to
open **all 180 partitions** and filter itself, instead of opening just the ~30 partitions of one month.

### 10.5 Known limitation
`DROP TABLE ... PURGE` prints the Lakekeeper error `BadRequestException: Malformed request` **after**
dropping, exactly as seen 7 times in the Bronze layer (`docs/data-platform/reference/spark.md` section 5f). The table really is gone:
`SHOW TABLES IN lakehouse.silver` returns empty. Purge runs **in the background**, so disk space is released a few tens of
seconds later. The cause is unknown and has not been investigated.

---

## 10.6 PROVEN: dedup over 10 consecutive days

The window chosen is **2026-04-05 .. 2026-04-14**, not arbitrary: `docs/data-platform/reference/silver.md` section 3.2 measured
that the two schema shapes overlap at `export_date` **2026-04-07..04-12**. Taking a window that covers
that means one run proves **both** things the Silver job does.

The trace of the schema change is visible right in the source file sizes: 04-05..04-09 are ~**475 MB** per day,
then 04-10..04-14 drop to ~**418 MB**. The 2026 shape is more compact because it drops 5 keys compared with v0.

**The schema switch point is export day 100 = 2026-04-10**, set in
`offline_problems.amazon_orders_api_switch_day: 100` (`configs/default.yaml:23`,
`generator/channels/amazon.py:85-89`). The 180-day range splits into **99 v0 days + 81 days in the 2026 shape**.
A misleading name, written down so nobody misreads it again: `"2026-01-01"` is the **NAME** of the new schema
version (`amazon.py:48` `SCHEMA_2026 = "orders-2026-01-01"`), **not the switch date**.
The size drop 475 -> 418 MB exactly at 04-10 is independent evidence for that switch point.

```bash
bash scripts/run/run_order_snapshots_silver.sh dup        # run both steps; or one step at a time:
spark-submit ... bronze/ingest_amazon_order_snapshots.py --window-days 10 --window-start 2026-04-05 \
    --aqe on --table raw_amazon_orders_dup
spark-submit ... silver/build_amazon_order_snapshots.py \
    --source-table raw_amazon_orders_dup --table stg_amazon_order_lines_dup
```

### Measurements
| | Number |
|---|---|
| Bronze: orders loaded (10 days) | 5,827,514 — 19.6s |
| Silver: Order Lines before dedup | 8,449,302 |
| Silver: Order Lines after dedup | 8,314,312 |
| **duplicates removed** | **134,990** |
| **measured duplicate rate** | **1.5976%** — 136.7s |
| v0 shape / 2026 shape before dedup | 4,222,964 / 4,226,338 |

### Three statements in this document, now backed by NUMBERS instead of argument

**1. "Dedup cannot be done within each partition" (section 3.2).**
Grouping the 134,990 duplicate keys by number of distinct `export_date` values: **100% sit on EXACTLY 2 different days**,
and not a single key has two records on the same day. So dedup restricted to one partition would catch
exactly 0 records — not an estimate, a count.

**2. "Parse FIRST, dedup AFTER" (section 3.2) — this is the strongest evidence.**
Of the 134,990 duplicate keys:
  * **34,080 keys SPAN BOTH SHAPES** — one record is v0, one is 2026;
  * 100,910 keys are within a single shape.
For those 34,080 keys, before parsing the two records' keys have **different names**
(`AmazonOrderId` vs `orderId`), so dedup run before parsing **does not recognize them as the same
Order**. 34,080 is larger than the 23,487 recorded in section 3.2 because the window here (10 days) is wider than
the previously measured window (04-07..04-12).

**3. Tie-break on largest `export_date` (section 3.1) — illustrated by a real row.**
Key `111-2589375-9860289` / `21318856472150` has two records:

| shape | `channel_status` | `last_updated_time` | `export_date` |
|---|---|---|---|
| v0 | `Shipped` | 2026-04-07 21:22:54 | 2026-04-07 |
| 2026 | `SHIPPED` | 2026-04-07 21:22:54 | **2026-04-10** |

Both records have **the same `last_updated_time`** — i.e. the main tie-break TIES, exactly the situation section 3.1
anticipated. The Silver table keeps the record with `export_date = 2026-04-10`, i.e. the one with the largest
`export_date`. The rule works correctly on real data.

### Why 1.5976% rather than the 1.98% in the generator manifest
It is not a measurement error but a **window-edge effect**, and it can be computed: an Order exported on day
D is also re-exported on D+1..D+3 (`overlap_days = 3`). For Orders exported near the right edge of the window
(04-12..04-14), their duplicates fall OUTSIDE the window and are not counted. Losing about 3 of
10 days on one side => about 80% of the duplicate pairs remain, and 1.98% × 0.8 ≈ **1.58%**,
matching the measured 1.5976%.
=> Measuring a duplicate rate EQUAL to the manifest would require loading all 180 days. **Not measured.** The 1.5976% figure
is a lower bound, and it is enough to prove that dedup works.

## On AWS

DP2 runs Spark Compose on EC2, with Bronze/Silver/Gold on S3;
run `manual__2026-10-27T16:05:58+00:00` in the [receipt](../../evidence/aws/data-platform-aws-20261027.json).
The [Amazon job](../../../spark_jobs/silver/build_amazon_order_snapshots.py) parses the two schemas, then normalizes, and a
Window by key keeps the latest revision; the [DAG](../../../dags/dp2_silver.py) places a validator after each Silver job.

![DP2 summary: Total success 1, Total Tasks 14 and the list of green Silver, Gold, SCD2 tasks](../../evidence/aws/dp2-silver-summary-20261028.png)

![EC2 Session Manager: DP2 log records Silver validate PASS, Gold matching per Channel and Amazon SCD2 with no errors](../../evidence/aws/dp2-validate-logs-20261028.png)

The receipt records Silver Amazon 153,849,901, Walmart 15,562,675, TikTok Shop 33,338,110,
Shopify 17,782,786, Shopee 6,666,831 rows; Gold matches per Channel, with 0 errors and 0 duplicate Order Line keys.
The screenshots prove the DAG status and validator results. There is not yet an AWS Spark UI view opening the Union/schema execution plan and the Window dedup,
so it cannot yet replace the detailed processing proof for schema evolution and dedup. [Airflow AWS](../pipeline_orchestration.md#on-aws).
