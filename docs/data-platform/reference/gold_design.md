# Gold layer design — implemented decisions

This document records the facts and choices settled before building Gold.
The Gold fact, Amazon SCD2, features and label bridge have been verified in
[the runs](gold.md). Business vocabulary is kept in
[terminology and data layers](../../shared/terminology.md).

## Verified facts

- All five Silver tables store **one row per product in an order**. Each sales
  Channel has its own table.
- Amazon order data only has an **estimated delivery** window; there is no actual delivery time
  (`generator/channels/amazon.py`, `spark_jobs/silver/build_amazon_order_snapshots.py`).
- The data generator writes `"Delivered"` for both the **delivered** and **completed** statuses
  of Walmart (`generator/channels/walmart.py`). So `status_changed_at` may
  be the completion time and cannot be treated with certainty as `delivered_at` (delivery time).
- Answer data about returns (`label`), delivery time (`delivered_at`) and
  the time the answer becomes available (`label_available_at`) is kept separately in
  `ground-truth/labels`. The table `ground-truth/order_line_keys` maps internal IDs to each
  Channel's IDs (`generator/generate_order_snapshots.py`). The answer exists only after the 30-day deadline.
- Walmart's `purchase_order_id` was once duplicated across two different orders. To
  uniquely identify a product line you must use
  `purchase_order_id`, `customer_order_id`, `line_number` together (`docs/data-platform/reference/silver.md` section 3.1).

## ✅ Q1 — How granular is the first Gold table? (settled)

| Option | Benefit | Trade-off |
|---|---|---|
| **A. Merge the five Channels into `fact_order_lines`, one row per product in an order** | Used directly to compute revenue, as the feature table for prediction, and to join with descriptive tables | Identifiers and columns that differ across Channels must be normalized; larger table |
| **B. Build a daily revenue aggregate first** | Small table, easy to present in reports and to benchmark query speed | The detail table still has to be built afterwards to complete prediction and the table relationship diagram |

**Chosen: A.** Create `fact_order_lines`, one row per product in an
order across all five Channels. This table contains only information observable from the
order. The return answer is not put in the table, and the estimated delivery date is not used as the
actual delivery date. Once the detail table is stable, the daily revenue table can be computed
from it. Each amount keeps the source's currency unit; USD/VND are not summed
together without an exchange-rate table.

## Verified consequences of Q1

- None of the five Silver tables currently has a reliable actual delivery time column
  for **every** Order Line. TikTok has the status `DELIVERED`, but the
  snapshot's `update_time` may be a later status change; Walmart merges
  delivered and completed into `Delivered`. `delivered_at` exists only in the
  ground truth. So `fact_order_lines` has no column named `delivered_at`.
  Amazon/Walmart's estimated delivery times can be kept under the names
  `estimated_delivery_start` and `estimated_delivery_end`, not renamed as actual delivery.
- `fact_order_lines` uses a stable key from the Channel plus the original ID set; Walmart
  in particular must contain `purchase_order_id`, `customer_order_id`, `line_number` before
  the Gold key is created. This keeps two orders sharing a `purchase_order_id` separate.
- Ground truth is used only in DP3 when building the labeled dataset; it does not enter the fact.
  `label_available_at` must be less than or equal to the run's data cutoff.

## ✅ Q2 — When does DP3 predict? (settled)

| Option | Fields used as features | Trade-off |
|---|---|---|
| **A. As soon as the Order is placed** | Price, quantity, SKU, Channel, destination, order timestamp and history known before that | No delivery status or late update times; close to early decision-making |
| **B. When the Order has just been delivered** | Adds shipping/delivery signals if the Channel has them | The five Channels do not provide a reliable enough `delivered_at`; would need the ground-truth timestamp or accept uneven coverage |

**Chosen: A:** predict at the moment the Order is placed. The Gold table may store
the latest snapshot for reporting, but the DP3 feature job selects only columns actually
known at order time. The final `order_status` and `last_updated_time`
are not used as features.

## ✅ Q3 — How are Walmart labels joined when order IDs collide? (settled)

The mapping `ground-truth/order_line_keys/walmart` has only
`channel_order_id` and `channel_line_id`; the Walmart payload also has
`customer_order_id`. It was confirmed that ID `1678172477013` maps to two different internal
Orders (`ORD-011-539381-1` and `ORD-042-53671-1`) on the same line 1. Joining
on the ID pair in the mapping would assign wrong labels or duplicate rows.

| Option | Approach | Trade-off |
|---|---|---|
| **A. Add `customer_order_id` to the label key table using the deterministic ID function and the seed of the source generation run** | Joins exactly on all three keys, keeps colliding rows | Depends on the generator's ID rule; must be documented as a bridge for simulated data |
| **B. Drop ambiguous Walmart keys from the labeled set** | Simple, independent of the ID-generation algorithm | Loses some Order Lines from the train/evaluation set and requires counting the dropped rows |

**Chosen: A.** The `order_line_id` in the mapping has the form internal Order ID
plus `-line_number`, so the Order ID can be recovered, `customer_order_id` recomputed
with `native_digits(seed, 'walmart_co', order_id, 13)` as used in the renderer,
then compared with the source payload. The job must stop if a key still joins multiple rows.

`scripts/run/prepare_walmart_label_keys.py` builds a four-key-column Parquet from the
ground-truth mapping, using the seed in `/data/raw/run_manifest.json` and exactly the generator's
`native_digits` function. This step runs on the host because the Airflow driver uses
Python 3.11 while the Spark worker uses Python 3.10; a Python UDF in Spark fails with
`PYTHON_VERSION_MISMATCH`. The code `spark_jobs/gold/build_walmart_order_snapshot_labels.py` reads the
prepared Parquet, joins all three original keys with the Gold fact, and only then takes labels from the
ground truth. The label table is separate from the Gold fact and the feature table so the answer does not
leak into Order placement time. The bridge ran successfully with 1,319,345 rows,
no duplicate keys or bad rows; see [runtime evidence](gold.md).

## ✅ Q4 — History source for the SCD2 dimension table

The SCD2 dimension table uses `valid_from_ts`, `valid_to_ts`, `is_current`
and a relationship with the fact. The `seller.skus` source is currently a static catalog; the price on an Order
Line is the transaction price and cannot be treated as Listing price history. Amazon Bronze has
overlapping exports of the same Order, Silver keeps the latest, so Bronze
can provide the actual Order Status changes of the simulated data.

| Option | Approach | Trade-off |
|---|---|---|
| **A. `dim_amazon_order` with Order Status history from Amazon Bronze** | Create a version when the observed status changes across Export Dates; join with the Amazon fact on the original Order ID | Proves SCD2 only for Amazon; effective timestamp settled per Q5; the first export date that observed the version is kept separately |
| **B. `dim_sku` from the static Seller catalog** | Clear SKU relationship across all five Channels | No real attribute changes in the current source; new source history must be added before SCD2 can be proven |

**Proposed: A:** it has source history actually delivered to the system and avoids inventing SKU
versions. **Chosen: A.**

## ✅ Q5 — SCD2 version start timestamp

After Q4, checking `generator/channels/amazon.py` showed that each Amazon snapshot writes
`LastUpdateDate` or `lastUpdatedTime` from the `change.at` of the currently visible status.
So there are two real source timestamps for `valid_from_ts`:

| Option | Meaning of `valid_from_ts` | Trade-off |
|---|---|---|
| **A. Status change time (`LastUpdateDate` / `lastUpdatedTime`)** | Business time per the Amazon snapshot; suits querying status as of an Order Line time | Only statuses present in loaded exports become versions; intermediate steps never seen remain missing |
| **B. Export date that observed the status (`export_date`)** | When the system learned about that version | Loses hour-level precision; the export date can be several days after the status change |

**Proposed: A:** the simulated source records the status change time directly. Also keep
`first_observed_export_date` to distinguish business time from observation
time. **Chosen: A.** Statuses that never appeared
in Bronze still cannot be recovered; they are not filled from the generator's history.

## ✅ Q6 — Attaching the dimension version to the Gold fact

The Amazon Gold fact currently keeps `channel_order_id` and `last_updated_time` from the latest
Silver snapshot. The SCD2 table will have multiple versions per Order, so joining only
on the Order ID would multiply rows. Both options use exactly the version timestamp
settled in Q5:

| Option | Join method | Trade-off |
|---|---|---|
| **A. Add `amazon_order_version_key` to the fact and the dim** | Stable key from the Order ID and the status change time; the Amazon fact points to exactly one version, other Channels null | The Gold fact must be rewritten and Validate/DP3 rerun to get the new schema |
| **B. Keep the existing fact, join on Order ID + time in the SQL query** | No need to rewrite the fact | Every query author must remember the time condition; no explicit version key in the schema |

**Proposed: A:** the dim/fact relationship can be key-checked and shown clearly in the schema
documentation. **Chosen: A.**

## SCD2 implemented and verified

- `lakehouse.gold.dim_amazon_order_dp2` has one row per **status version
  of an Amazon Order that appeared in Bronze**. The source is
  `raw_amazon_orders_dp1`; both the v0 and 2026 schemas are parsed as in Silver, at Order level
  so as not to multiply by Order Line.
- The version key is stable from `(order_id, last_updated_time)`; it keeps
  `order_status`, `channel_status`, `first_observed_export_date`,
  `valid_from_ts`, `valid_to_ts`, `is_current`. `valid_from_ts` is the time
  the status changed in the payload. `valid_to_ts` is the `valid_from_ts` of the next
  version (**exclusive** bound); the last version has `valid_to_ts = NULL`
  and `is_current = true`.
- Multiple exports with the same Order and the same update time are merged into one
  version. The two Amazon schemas may write the same canonical status differently
  (`Shipped` and `SHIPPED`); `channel_status` keeps the source text from the latest export.
  If the **canonical** status conflicts, the job stops instead of picking arbitrarily.
  A status change between two exports that was never observed does not
  appear; the generator's hidden history is not used to fill it.
- The Amazon Gold fact adds `amazon_order_version_key` from the key above; the other four Channels
  are null. Validate requires every Amazon Order Line to join **exactly one**
  version in the dim. The DP3 feature table does not include this status or dimension key
  as features, because prediction happens when the Order is placed.
- After rerunning Gold/Validate and DP3/Validate, update the screenshots and runtime numbers
  in place of the old evidence if the output changed. The Walmart label table is separate and
  its schema is unchanged.

## On AWS

DP2/DP3 Spark on Compose EC2 wrote Gold to S3; [Gold on AWS](gold.md#on-aws)
embeds the validator log and links the fact, SCD2 and feature code.
The [AWS manifest](../../evidence/aws/datahub-validation-aws-20261029.json) records no missing SCD2 versions,
no duplicate fact keys, and no wrong feature event times in the recorded runs.

The logical relationship is `fact.amazon_order_version_key → dim.order_version_key`; the feature joins the fact on `order_line_key`.
SCD2 uses `valid_from_ts`, `valid_to_ts`, `is_current`; the feature separates `event_timestamp` and `created` per the design above.
There is no AWS metadata/ERD capture yet to visually confirm the columns and relationships; the local schema screenshots remain only a reference.
