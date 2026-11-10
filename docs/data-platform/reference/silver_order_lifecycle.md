# Bronze/Silver for the v2 source — package C

> **Pipeline v2 (order lifecycle) — designed and tested locally, not the deployed pipeline.** The pipeline running on AWS is v1 (DP1 → DP4 + ML), see [Data Platform evidence](../evidence.md). Mentions of GCP below are an old plan that was never run; the current cloud target is [AWS](../../shared/aws_deploy.md).

Loads the small v2 source set from [package B](source_order_lifecycle_fixtures.md) into the lakehouse according to
the [v2 source contract](source_contract_order_lifecycle.md). This is a **small fixture** (5 Channels,
3 order-creation days, 15 Orders, 30 Order Lines), not the full 100 GB
dataset. The `_dp1`/`_dp2` tables used as earlier evidence **are not touched**: the v2 source
writes to the new namespaces `ingest_bronze` and `build_silver`.

## Design

| Layer | Table | Grain | Notes |
|---|---|---|---|
| Bronze | `bronze.lifecycle_raw_records` | One source row | **One shared table** for every feed, keeping `raw_json` as is, partitioned by `feed`. Adding a feed or changing a schema does not require altering the table. The `ingestion_boundary` column records `file` or `jdbc` |
| Silver | `lifecycle_stg_line_commitments` | Order Line (latest revision) | Ordered/canceled quantities, ordered/canceled amounts, `headline_product_net_minor` = ordered − canceled before shipping |
| Silver | `lifecycle_stg_allocations` | Allocation | One line can have several allocations (split delivery 2+1) |
| Silver | `lifecycle_stg_deliveries` | Delivery event | Quantity actually delivered per allocation |
| Silver | `lifecycle_stg_return_events` | Return event (history) | requested → approved → received |
| Silver | `lifecycle_stg_refunds` | Refund transaction (latest revision) | Refunds are kept **separate**, not subtracted from headline; a refund not yet attached to a line keeps `unresolved_scope_ref` |
| Silver | `lifecycle_stg_listing_versions` | Listing revision (history) | `is_latest` marks the version in effect; a correction replaces the wrong version, and the wrong version is kept |
| Silver | `lifecycle_stg_stock_operations`, `lifecycle_stg_stock_snapshots`, `lifecycle_stg_inbound_events` | Stock operation / snapshot / inbound | Shared Seller pool |
| Silver | `lifecycle_stg_uncertainties`, `lifecycle_stg_coverage` | Uncertainty / source coverage | Read from the producer's auxiliary feeds |
| Silver | `lifecycle_stg_line_lifecycle` | Order Line | Aggregates allocation/delivery/return/refund **to line grain before joining**, so there is no fan-out |
| Silver | `lifecycle_stg_native_order_lines` | Line in a native order | Parses the native shape of the 5 Channels (Amazon with two schemas) into common columns: quantity, gross amount, amount after discount. Walmart does not send the gross price, so `gross_minor` is null |
| Silver | `lifecycle_stg_coverage_receipts` | Coverage envelope | Result of applying the producer's coverage to exactly the set of rows received, with the reason if anything is missing |
| Silver | `lifecycle_stg_observed_evidence` | One observation record | Label feeds + coverage + uncertainty that passed the contract gate, **every revision** (only exact retry duplicates are dropped); keys `ev_line_id`/`ev_order_id`/`corrects_source_event_id` let Gold group by line. Partitioned by `days(source_observed_at)` |
| Silver | `lifecycle_stg_coverage_validity` | Coverage revision | `complete`/`partial` according to the rules in `spark_jobs.lib.return_label.source_line`, checked once for the whole dataset |
| Silver | `lifecycle_stg_build_info` | One build | `run_id`, `as_of`; Gold v2 refuses to run if `as_of` ≠ cutoff |

Shared rules (code in [`spark_jobs/lib/order_lifecycle_lakehouse.py`](../../../spark_jobs/lib/order_lifecycle_lakehouse.py)):

- **Event identity** = `(dataset_version, producer, source_epoch, source_event_id)`.
  Repeats with the same content are dropped; the same id with different content is a **conflict**, which is excluded
  from Silver and makes the job fail. The content hash ignores `recorded_at` because that is the time of the machine
  running it, not a source fact.
- **Highest revision wins**: a correction replaces the corrected record; it is not added on top.
  Example: correcting opening stock 200 → 199 makes the stock ledger use 199, not 399.
- **`as_of`**: only rows with `source_observed_at` before that time are used, to rebuild
  exactly what was known at that point. Native Order snapshots (which have no `source_observed_at`) are treated as known at
  **the end of the export day** (`export_date` + 1 day). The first version of these rows was filtered out when `as_of` was set ⇒
  `seller_sku` of every line became null (a bug that only shows when the v2 DAG passes `--as-of`).

## Walmart over JDBC

The manifest declares Walmart as another department's DB (**JDBC** boundary); the JSONL file is only
a bootstrap for loading the DB. Therefore:

1. [`scripts/setup/load_order_lifecycle_walmart.py`](../../../scripts/setup/load_order_lifecycle_walmart.py) loads the bootstrap
   file into the new table `walmart_lifecycle.orders` (key `dataset_version` +
   `purchase_order_id`). `raw_payload` is **TEXT**, not JSONB, so the DB does not reorder
   the keys of the original string. Reloading the same `dataset_version` deletes then inserts within
   one transaction, without duplicating. The v1 table `walmart.orders` is not touched.
2. The job `spark_jobs/bronze/ingest_order_lifecycle.py` with `--walmart-jdbc walmart_lifecycle.orders` drops the Walmart rows from the file, reads them over
   JDBC using `json_build_object(...)::text`, and fails if the set of order IDs differs from
   `native_sources.walmart.snapshot_scope.order_ids` (missing, extra or duplicate).

## Reconciliation — every check must be 0

The job `spark_jobs/silver/build_order_lifecycle.py` reconciles against **the tables just written** (reread from the
catalog); any check that is not 0 makes the job fail.

| Check | Meaning |
|---|---|
| `event_conflicts` | Same event id with different content, or same entity/revision with a different id |
| `missing_dependencies` | Event points to an original event that does not exist (`depends_on`, `corrects`) |
| `duplicate_lines`, `orphan_allocations` | Duplicate line; allocation without a line |
| `quantity_mismatch_lines` | Finished line where ordered ≠ canceled + allocated |
| `delivery_mismatch_allocations` | Delivered allocation where delivered ≠ allocated, or over-delivered |
| `fanout_line_rows`, `fanout_headline_delta_minor` | The line lifecycle table differs in row count or headline total from the line table |
| `refund_unscoped_rows` | Refund not attached to a line that does **not** keep an uncertainty scope (silently dropped) |
| `refund_exceeds_headline_lines` | Successful product refund exceeds the line's headline |
| `stock_snapshot_mismatches` | Cumulative stock operations up to the snapshot time ≠ snapshot |
| `listing_binding_orphans` | Line sold through a Listing mapping that does not exist |
| `native_unparsed_rows` | Native order whose order ID/line ID cannot be parsed |
| `native_lines_without_commitment`, `commitments_without_native` | Line present on only one side: the Channel says it exists but the Seller did not record it, or vice versa |
| `native_quantity_mismatches`, `native_money_mismatches` | Quantity / amount after discount / currency of the native order differs from the OMS **original ordered** values (native is a snapshot at order creation, so it is not compared with post-cancellation values) |
| `coverage_receipt_failures` | A `complete` envelope where the received row set is missing/has extra sequence numbers, has the wrong row count, falls outside the event range, or the whole-set hash differs from `record_set_sha256` |
| `feeds_without_complete_coverage` | Feed with events but no passing coverage |

`unavailable` coverage (pool scope managed by the Channel, with no access to the operational source)
is not checked but counted in `info.coverage_unavailable_scopes`; there the data is
**unknown**, not 0.

## Results

**Spark tests** (27 cases, image `platform-airflow:2.10.5-spark3.5.3`, `local[1]`, no
network, read-only workspace, fixture freshly generated in `/tmp`): **27/27 OK**. Besides the clean
case, the tests deliberately corrupt the data and check that the checks catch it: retry repeats,
same id with different content, a missing original event, a missing second allocation, a stock
correction, a mismatched Walmart amount, a missing Shopee order, a missing stock row (coverage reports
`missing_sequence` + `record_count_mismatch`), a silently altered delivery row
(coverage reports `record_set_digest_mismatch`); Amazon schema 2 parses to the same
result; Walmart rows in JDBC form (JSON with whitespace) replacing the file rows still
parse and reconcile cleanly; and checking `as_of` before a refund correction finds **15** unresolved refunds that
keep their affected scope, and **0** after the correction.

```bash
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=1g \
  --mount type=bind,src=$PWD,dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_order_lifecycle_silver.py
```

**Real run against Lakekeeper/MinIO** on the `default` variant
(`dataset-9b9767a38a44fce25446af2afb6836e6`):

- Loading PostgreSQL twice in a row: file 3 = DB 3 both times.
- Bronze: manifest **550** source rows = Bronze **550** (file 547 + **JDBC 3**), errors 0;
  Walmart JDBC missing/extra/duplicate 0; run `bronze-v2-20261018-jdbc`
  ([receipt](../../evidence/bronze-v2-jdbc-20261018.json)).
- Silver: 489 operational events, 0 repeats, **30** Order Lines, **45** allocations,
  headline **750,000** minor USD (= 15 × (20,000 + 30,000)), refunds not attached to a
  line 0, unresolved uncertainties 0; **30** native lines match 30 OMS lines;
  coverage **9** complete, **6** scopes unavailable; **19/19 checks are 0**, run
  `silver-v2-20261018-jdbc`. Receipt: [`silver-v2-receipt-20261018.json`](../../evidence/silver-v2-receipt-20261018.json).

```bash
env -u POSTGRES_PASSWORD docker compose --profile catalog up -d postgres minio lakekeeper
export FIXTURE=out/source-v2-b-final-20261018-051930425055/default
env -u POSTGRES_PASSWORD .venv/bin/python scripts/setup/load_order_lifecycle_walmart.py --root $FIXTURE
scripts/run/run_order_lifecycle_spark.sh /opt/spark_jobs/bronze/ingest_order_lifecycle.py --root /data/order_lifecycle \
  --run-id bronze-v2-20261018-jdbc --walmart-jdbc walmart_lifecycle.orders
scripts/run/run_order_lifecycle_spark.sh /opt/spark_jobs/silver/build_order_lifecycle.py --run-id silver-v2-20261018-jdbc \
  --receipt /receipts/silver-v2-receipt-20261018.json
```

The fixture lives in `out/` (ignored by Git); regenerate it with
`python -m generator.generate_order_lifecycle_fixture --output <new directory>` per [package B](source_order_lifecycle_fixtures.md).

## Limitations — not done in this step

- Walmart JDBC is read in one pass, not split into parallel partitions like the v1 job (the fixture
  has only 3 orders); there is no snapshot receipt from the DB itself yet (LSN/txid).
- Coverage is checked in the job, but there is no publish step to block on it yet; currently the job
  only reports an error.
- Native amounts are converted from decimal strings at scale 2 (USD in the fixture);
  the currency table is not read yet to handle currencies with a different scale.
- There is no Gold, label v2 (package E), Airflow DAG or DataHub for the v2 tables yet.
- Bronze overwrites the whole table on every run; it does not load incrementally per run yet.
- Running with `--as-of` overwrites the same Silver tables; the `as_of` evidence currently lives in
  the Spark tests, with no separate point-in-time tables yet.
