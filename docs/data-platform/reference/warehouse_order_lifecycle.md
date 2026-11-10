<!-- Warehouse PostgreSQL v2: model, safe publish and the process for collecting real evidence. -->
# Warehouse v2 on PostgreSQL

> **Pipeline v2 (order lifecycle) — designed and tested locally; not the deployed pipeline.** The pipeline running on AWS is v1 (DP1 → DP4 + ML), see [Data Platform evidence](../evidence.md). Mentions of GCP below are an old plan that never ran; the current cloud target is [AWS](../../shared/aws_deploy.md).

This part measures warehouse indexing. The Walmart v1 slice does not represent
the five Channels and loses its indexes after overwrite. The job `spark_jobs/warehouse/export_order_lifecycle.py` reads `build_gold` and writes the new schema
`warehouse_lifecycle` on the VM's PostgreSQL service. v1 is kept unchanged; currencies are not converted or
summed together. For VM/storage/auth configuration see the [AWS runbook](../../shared/aws_deploy.md).

| Table | Grain and retained data |
|---|---|
| `fact_order_lines` | PK `(dataset_version,line_id)`; every real Gold column, five-Channel identity, currency, money as minor-unit bigint, ordered_basis_qty/canceled_unshipped_qty/allocated_qty/delivered_qty, headline and product refund kept separate |
| `label_return` | PK `(dataset_version,line_id,cutoff,generation_id)`; state, nullable label, label_version, computed_at, maturity/cohort, input_json, reasons and source_event_refs as text[] |
| `feat_sku_return_rate_90d` | PK `(dataset_version,line_id)`; SKU, event_timestamp, created, counts/rate, label_version and generation_id |

DDL [001](../../../sql/warehouse_lifecycle/001_tables.sql) takes its columns from
`spark_jobs/lib/order_lifecycle_gold.py` and `spark_jobs.lib.order_lifecycle_lakehouse.line_lifecycle`; Spark UTC timestamps
map to PostgreSQL `timestamp` (without time zone), quantities to integers and aggregates to bigint,
rates to double precision. Gross/discount, which do not exist in the Gold fact, are not added.
The warehouse label is the **generation/cutoff being exported**; the immutable history stays in Gold.
A CHECK keeps label NULL when state is not labeled; unknown is never turned into 0.

| Index from [002](../../../sql/warehouse_lifecycle/002_indexes.sql) | Query |
|---|---|
| PKs from 001, carried with staging on rename | Prevent grain duplicates, line lookup on join |
| `lifecycle_fact_sales(created_at) INCLUDE(channel,currency,headline_product_net_minor)` | Q1: 30-day headline, GROUP BY UTC date × channel × currency |
| `lifecycle_fact_sku(seller_sku,dataset_version,line_id) INCLUDE(generation_id)` | Q2: pick a SKU, then join fact/label on dataset, line and generation |
| `lifecycle_label_cutoff(cutoff,dataset_version,line_id,generation_id) INCLUDE(label) WHERE state='labeled'` | Q2: the denominator includes only labeled rows at exactly the cutoff, AVG(label) is the rate |
| `lifecycle_feature_pit(dataset_version,seller_sku,event_timestamp DESC,created DESC)` | Q3: the latest feature row with event_timestamp ≤ T and created ≤ the time the query knows the data |

The five Channels must be exactly `amazon,walmart,shopee,shopify,tiktok_shop` when exporting the fact.
Q3 keeps Gold's per-line feature grain, taking one row with line_id as tie-break; it does not
sum counts across lines. `created` is the availability at computation time, so it can be after
`event_timestamp`; a query at order placement time may find no feature yet and return 0 rows.
Choose an `--as-of` that fits the check's goal; do not implicitly treat a feature computed later as known at T.

## Safe publish

1. The CLI pins the current Iceberg snapshot of each table separately. Fact/feature must belong entirely
   to `--run-id`; labels are filtered to exactly the generation and cutoff; a missing snapshot or empty table
   fails closed. The DAG validates Gold before running the export.
2. Hold a session advisory lock shared by export/benchmark. Create regular staging tables
   `<table>__load_<sha256(run_id)[:12]>` from 001, including PK/CHECK, then append via JDBC.
   TEMP TABLEs are not used because Spark executors open multiple JDBC sessions.
3. Compare the logical Gold count with the actual staging count; for the fact, compare count and **four money totals
   per `(channel,currency)`**, using decimal/numeric in SUM to avoid bigint overflow.
   The receipt records expected/actual and a boolean per check, never credentials.
4. Only when all checks are true: in one transaction rename the old tables, rename the old indexes
   to free their schema-wide names, rename staging to the serving tables, apply 002,
   ANALYZE, then DROP the old copies. The whole group of tables is committed together. Any error rolls back the whole group.
5. On failure, delete the staging tables created by this invocation and leave the serving tables unchanged. If the
   process is killed, staging may remain: the operator checks/deletes exactly the failed staging tables
   before retrying with the same run-id; the job does not delete pre-existing staging tables by itself.

`--tables` is a comma-separated list; default all three. A subset publishes only the selected
group, so use the default for a full generation. Writers must use this process,
and there must be no views/FKs depending on the serving tables: PostgreSQL views/FKs bind to the old table's OID,
not its name; a DROP without CASCADE will refuse and roll back if there is a dependency.
This package does not grant per-table privileges: the swap creates new OIDs, so the operator uses the same
owner or default privileges granted at the schema level. The rename transaction needs table locks and may
wait for running queries; the benchmark also blocks writes for the whole measurement.

## Running locally and on the VM

The venv has `psycopg`; the Airflow image currently has `psycopg2-binary==2.9.10`. The module picks
one of the two drivers by itself; no extra dependency is installed. Spark needs the PostgreSQL 42.7.4 JAR
already present in the image/cache and the Iceberg runtime like the existing runner. Spark and
a real PostgreSQL have not been run in the sandbox.

Compose currently mounts only `spark_jobs/`, not `sql/`. Because this package does not modify Compose,
the operator copies the two migrations into the running Airflow container; no config change is needed:

```bash
# The operator runs this once the stack is ready; copy again after the container is recreated.
# SQL is mounted read-only at /opt/sql/warehouse_lifecycle.
# New DAG dp_order_lifecycle_warehouse: trigger manually with a validated generation of dp_order_lifecycle.
# {"run_id":"EXISTING_GOLD_GENERATION","cutoff":"2026-06-01T00:00:00.000000Z"}
```

The new DAG has `validate_gold >> export_warehouse`, schedule=None, one run at
a time; the source DAG is not modified. The default SQL dir is `/opt/sql/warehouse_lifecycle`, which can be
overridden with the Airflow Variable `platform_order_lifecycle_warehouse_sql_dir`. Receipts go under the Variable
`platform_order_lifecycle_receipt_dir`/warehouse/TS_NODASH. On GCP the SQL is also copied with the command above
in `/opt/platform`, using PostgreSQL on the VM and the credentials of the Connection `platform_postgres`.

CLI outside the DAG (in an image/runtime that has Spark and the catalog):

```bash
spark-submit --jars /CACHE_PATH/postgresql-42.7.4.jar \
  spark_jobs/warehouse/export_order_lifecycle.py \
  --run-id EXISTING_GOLD_GENERATION --cutoff 2026-06-01T00:00:00.000000Z \
  --receipt /tmp/warehouse-v2-export.json --sql-dir sql/warehouse_lifecycle
```

The current runtime's Iceberg/GCS/MinIO configuration/JARs are also needed; the CLI illustrates the JDBC part
and the arguments and does not replace the catalog runner. Connection settings come from env, like the job `spark_jobs/warehouse/export_order_snapshot_lines.py`:
`POSTGRES_HOST` (postgres), `POSTGRES_PORT` (5432), `POSTGRES_DB` (sources),
`POSTGRES_USER`, `POSTGRES_PASSWORD`. Do not put secrets in arguments/logs.

## Temporary PostgreSQL 16 harness — run by the operator

Use a **separate empty** DB; the harness refuses an existing warehouse_lifecycle schema. The operator
prepares a `postgres:16` container on loopback or a separate Docker network and passes the
URI via the env `WAREHOUSE_TEST_DSN`; do not use a serving DB. The Python runtime
needs PySpark 3.5.3, Java and a PostgreSQL Python driver already available. No JARs are downloaded:

```bash
# This env holds the temporary DB URI: postgresql://USER:PASSWORD@127.0.0.1:PORT/DB
# The operator sets WAREHOUSE_TEST_DSN in the environment and does not write it into the report.
PYSPARK_SUBMIT_ARGS='--jars /CACHE_PATH/postgresql-42.7.4.jar pyspark-shell' \
  python tests/spark/check_order_lifecycle_warehouse.py
```

The harness builds fixture → Silver → real Gold, load 1 succeeds; a load with wrong staging money
is blocked and every serving-table value keeps version 1; a DDL error mid-swap also
rolls back to version 1; the next valid load still has the four secondary indexes. Bad staging is cleaned up,
and the temporary DB keeps the last valid version for further checks. The operator manages the container lifecycle.

## EXPLAIN before/after on the same data

```bash
# POSTGRES_* env points to the DB to measure; pick real values present in the serving Gold.
.venv/bin/python scripts/evidence/measure_order_lifecycle_warehouse.py \
  --cutoff 2026-06-01T00:00:00Z --sku REAL_SELLER_SKU --dataset REAL_DATASET_VERSION \
  --event-timestamp 2026-04-01T00:00:00Z --as-of 2026-10-19T00:00:00Z \
  --receipt /tmp/warehouse-v2-explain.json
```

The script takes the advisory lock, a SHARE lock on all three tables, REPEATABLE READ; drops **every secondary
index** of the three tables (keeping the PKs), runs ANALYZE, then measures Q1–Q3 five times each. It then creates
002 and measures five more times. All DROP/CREATE/ANALYZE are rolled back at the end;
indexes existing before the measurement are restored even on error. enable_seqscan=false is not forced.

The JSON has each full plan, the root node, actual rows, shared read/hit/total blocks and
the median of each metric. The root node already includes child buffers; they are not added again. Compare **the plan and the
median shared hit+read**, plus read/hit separately: the later phase may have a warmer cache,
so a drop in shared reads is not to be read as the whole index benefit. Timing is not used as
the primary proof. A small fixture, a low-selectivity range or a query returning 0 rows may use
Seq Scan in both phases; record the actual result and do not claim the index is always faster.

**e2e small local** measurements, after the fix and the rename; source 45 days ×
50 orders, 100 SKUs, PASS 92 s. The source numbers and run details are in [e2e local](e2e_order_lifecycle_local.md),
confirmed by the operator in the local notes (receipt not attached to the repo).
Warehouse checks on real PostgreSQL **PASS 116 s before the rename**: load, money-mismatch blocking,
DDL rollback and swap keeping indexes; this is not a speedup measurement.

| Query | Before: node / buffers hit+read / rows (median n=5) | After: node / buffers hit+read / rows (median n=5) |
|---|---|---|
| Q1 headline date × Channel × currency | Aggregate / 173 / 0 | Aggregate / 2 / 0 |
| Q2 SKU return rate at cutoff | Aggregate / 583 / 5 | Aggregate / 583 / 5 |
| Q3 SKU feature PIT | Limit / 104 / 1 | Limit / 4 / 1 |

Q3 drops 104 → 4 buffers thanks to the index; Q2 is unchanged. Q1 drops 173 → 2 but the window
is empty (0 rows), so it does not prove an index benefit. Medium has not reached EXPLAIN because the
pre-fix run hit OOM in gold_validate; it has not been rerun after the fix.
Measuring on GCP: `bash tests/harness/run_order_lifecycle_scale.sh --profile medium --driver-memory 6g`
on a prepared host; measure the serving DB with the `measure_order_lifecycle_warehouse.py` command
above with real parameters. Save the JSON with run/cutoff and real plan screenshots; there are no GCP numbers yet.
