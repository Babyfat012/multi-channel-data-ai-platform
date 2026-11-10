# Iceberg maintenance — policy-driven self-maintenance for Iceberg

Package H adds metadata triggers, generation retention, retries and post-checks to the
[acceptance ledger](../evidence.md). Manual compaction already exists
in the job `spark_jobs/maintenance/compact_amazon_order_snapshots.py`; this document does not reuse those screenshots/measurements as maintenance evidence.
Per [Spark section 5b](spark.md), timing noise was once measured at about 14.5%; the primary metrics
here are file count, probe candidate files, snapshots, row count and checksum.

## Components and decisions

`spark_jobs/lib/maintenance_policy.py` does not import pyspark: metadata dict + configuration + UTC
time → a JSON plan with a reason for each action, the protected snapshots and the generation
tags. `spark_jobs/maintenance/maintain_iceberg_tables.py` reads `.files`, `.snapshots`, `.refs`, `.partitions`
and `SHOW TBLPROPERTIES`, calls the policy, then preflights the SQL before executing. The
production session uses `spark_jobs.lib.iceberg_session.build_spark`; local uses a separate Hadoop catalog.
No dependency is added, and no jar is downloaded automatically in local tests.

| Order | Kind | Trigger | Execution |
|---|---|---|---|
| 1 | compact | A partition has at least min_input_files and average bytes below small_file_ratio × target_file_size_bytes | rewrite_data_files, binpack, where restricted to the partitions over the threshold |
| 2 | zorder | zorder_columns is set and there is no marker yet, or at least zorder_min_new_files since the marker | rewrite_data_files, sort, zorder(c1,c2), rewrite-all=true; writes the new snapshot id to platform.maintenance.last_zorder_snapshot_id |
| 3 | expire_snapshots | Snapshot older than the age limit, outside retain_last and not referenced by any ref | expire_snapshots with UTC older_than, retain_last |
| 4 | remove_orphans | Always inventoried; older_than at least 72 hours | remove_orphan_files, dry_run=true by default; only an explicit false deletes |

New files are counted by following parent_id from the main snapshot back to the marker, not counting other branches.
If the marker is no longer in the ancestry → z-order again, conservatively. `rewrite-all=true`
ensures the z-order step still sorts files that were just binpacked; a file being large enough does not mean it has
been sorted by content.

**Planning rule:** the requirement that remove_orphans is always in the plan
conflicts with the requirement that the second plan is empty. The implementation keeps the dry-run inventory;
`no_op=true` and `reason="no-op: ..."` when no table-changing action remains.
`actions` then contains only the remove_orphans dry-run, not an empty array. The tests state
this exception explicitly instead of pretending both requirements can be met at once. With
orphans_dry_run=false, there is always one delete action per the configured rule.

## Generation retention

A generation is a tag with the default prefix `gen-`, changeable via generation_tag_prefix.
The policy lists `protected_snapshot_ids` from **all** tags/branches and does not put them
into `expire_snapshots.snapshot_ids`. `generation_tags` is the list of tags whose
checksum must be checked. `main` is also a protected branch.

Before any procedure, the job reads each generation via `VERSION AS OF '<tag>'` and stores
count/checksum/schema. Right after expiry, and again at the end of the job, it rereads each tag and
compares with before. Unreadable or wrong checksum → fail. Iceberg remains the layer that
enforces retention: the procedure uses older_than/retain_last and handles refs the Iceberg way;
the candidate list in the policy must not be treated as the number of snapshots certainly deleted.
The job does not modify a tag's retention and does not delete tags/branches. The job refuses to run expiry
if a ref TTL (max_reference_age_in_ms or history.expire.max-ref-age-ms) could
make Iceberg drop a protected ref by itself. This check runs before any procedure and
again right before the expiry CALL; the post-check is still needed to detect concurrent changes.

## Retry, errors and receipt

`execute_actions` takes a procedure callback and a sleep function; pytest uses a fake callback.
CommitFailedException is retried. ValidationException is retried only when the message
indicates concurrent/conflicting/found new/cannot commit/changed/already been committed.
A wrong schema validation is not retried. Default maximum 3 attempts, backoff 1s, 2s,
capped at 8s. The receipt stores kind, attempt, status, error, transient, delay, result.
A fatal error or exhausted attempts → stop immediately, do not run the next action/table, write a
failed receipt and exit non-zero. Earlier actions that already committed are not rolled back.

`--inject-transient-failures 2` fakes two errors **before the CALL** of the first action, then tries
a third time. `--inject-fatal-failure` fails before the first CALL. Multi-table injects only
on the first table. The JSON receipt is written atomically, with UTC timestamps, the sha256 of the two code modules,
the sha256 of the configuration actually used, the plan, the SQL, each attempt and all before/after numbers.

## Post-checks and how they are measured

Within the same execute job, both before and after measure:

- `count(*)` over the logical relation, not the record_count metadata.
- Fingerprint: the decimal(38,0) sum of `xxhash64` over **all columns in schema order**.
  Count, checksum and column list must be identical; the sum is independent of row/file order and
  avoids bigint overflow. This is a probabilistic checksum and can collide.
- Number of current data files (`content=0`), total bytes, number of snapshots actually read from metadata.
- With zorder_columns and probe_predicates: the number of files whose lower_bound/upper_bound intersect
  every closed AND range of the probe; if a bound is missing, that bound is not used to exclude
  the file. A file is excluded only with evidence that the ranges do not intersect. Dates/timestamps are normalized
  to the metadata type, timestamps to UTC.
- Each probe runs count once as warmup + 3 measured runs; the receipt keeps each number of seconds, n=3.
  Time is not used to decide pass/fail or to claim a speedup.

These are **candidate files by min/max**, not instrumentation of the number of files Spark
actually opens: Spark may also use partition pruning/row groups/delete files. Metrics
must be enabled for the relevant columns; truncated bounds give a conservative estimate. A configured
predicate looks like `{"name":"sku-0001","ranges":{"seller_sku":["sku-0001","sku-0001"]}}`.
null means an unbounded end of the range; the probe schema currently supports closed ranges, no OR/LIKE.

`--validate-receipt <execute.json>` runs in a new Spark session, accepts only a success
receipt, checks that before/after were equal, then rereads the table/tags and compares checksum/schema.
It does not overwrite the original receipt. If another writer changes the data during the
maintenance/validate window, the checksum differs and it fails; choose a low-write window to read
consistent evidence. The job does not lock writers or provide a multi-action transaction.

## Airflow and operations

The new DAG `maintenance_iceberg` has schedule @daily, catchup=False,
max_active_runs=1, is_paused_upon_creation=True. Three tasks use
PlatformSparkSubmitOperator: plan (--plan-only) → execute (--execute, with post-checks) →
validate (--validate-receipt, new session). Execute rebuilds the plan from the current
metadata; it never blindly executes an old plan.

Variables added in `scripts/setup/bootstrap_airflow_config.py`:

- platform_maintenance_tables: JSON array of fully qualified catalog.namespace.table names;
  by default only the demo `lakehouse.demo.maintenance_demo`.
- platform_maintenance_receipt_dir: `/opt/airflow/logs/iceberg-maintenance`, on the
  Airflow volume; each logical timestamp has its own plan.json and execute.json.

Compose currently does not mount configs/. So `scripts/setup/setup_airflow.sh` **only adds** a step that
loads the whole configs/iceberg_maintenance.json via stdin into the Variable
platform_maintenance_config. The DAG passes the content with --config-json. This step
does not change other variables' configuration or Compose. After changing the JSON, rerun setup
or update exactly that Variable. Do not put secrets in the maintenance configuration.

The CLI outside the DAG can use `--config /path/to/iceberg_maintenance.json` with tables
in the JSON allowlist. Local: `--catalog local --local-warehouse /tmp/warehouse`,
jars provided via spark-submit --jars from cache. `--help` does not need pyspark.

## Tests and real measurements

The pure Python pytest suite has run; the full output is in the
report (internal note, not in the repo). The first tracer was red because the module did not exist yet, then green;
the additional rounds had reds due to a missing config/probe function and an ISO timestamp comparison bug,
then all went green.

**Spark local: run (by the operator) — `Ran 3 tests in 39.176s OK`.**
Full receipt: [`docs/evidence/iceberg-maintenance-local-20261018.json`](../../evidence/iceberg-maintenance-local-20261018.json)
(the sha256 of the two code files in the receipt matched the files in the repo when it was saved).

| Measured on table `maintenance_normal` (8,192 rows, 2 partitions) | Before | After |
|---|---|---|
| Data files | 32 | 12 |
| Snapshots | 32 | 4 |
| Files to read for probe `seller_sku = 'sku-0001'` | 32 | 2 |
| Whole-table fingerprint `sum(xxhash64)` | 253838296152053650978 | 253838296152053650978 (identical) |
| Tag `gen-1` fingerprint (4,096 rows) | 106188931843360907061 | 106188931843360907061 (identical) |

- Second planning pass: `no_op = true`, only the `remove_orphans` dry-run remains (no table change).
- Retry: `compact` fails transiently twice (simulated `CommitFailedException`) → succeeds on the 3rd attempt, the following steps all run.
- Fatal: error on the first action → stop, nothing further runs, the receipt has `status = failed` with the error.
- Probe time is printed in the receipt but is **not** used as evidence (noisy); the evidence is the number of files to read.

`tests/spark/check_iceberg_maintenance.py` prepares a Hadoop catalog under /tmp, the
Iceberg CALL extension and the 1.11.0 jar in ivy. Three independent cases create a table with 32 small appends,
2 identity partitions, seller_sku/created_at, tag gen-1 after append 16. The tests
require fewer files and probe files, unchanged data/tags, expiry reducing snapshots, a second pass with
no mutation, a retry of two errors followed by success, and a fatal error leaving snapshots unchanged. Validation
uses newSession and a JSON roundtrip. The retention test uses an age of 1 second, waits 1.1 seconds
so the initial data is older than the threshold, retain_last=3 to keep the last append + compact +
zorder; the clock is not mocked to manufacture idempotence evidence.

The operator runs:

```bash
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=2g \
  -v platform_airflow-data:/ivy:ro \
  --mount type=bind,src="$PWD",dst=/workspace,readonly --entrypoint python \
  platform-airflow:2.10.5-spark3.5.3 /workspace/tests/spark/check_iceberg_maintenance.py
```

The suite prints `ICEBERG_MAINTENANCE_EVIDENCE_JSON=...` only after all tests are green. The operator can save the
real JSON with code_sha256 into new evidence; the test does not write to the read-only repo mount.
Measurements depend on real Spark/Parquet file sizes: the assertions are **not yet
confirmed** in the sandbox. Compiling or pytest does not count as having checked the CALL API.

## Limitations and running on real tables

Compaction supports identity and time (year/month/day/hour) partitions via predicates
on the source column. Bucket/truncate, nested source column names and partition evolution
are not fully supported: the preflight rejects unsuitable mappings before the CALL.
Partition files are collected as JSON; identity dates use a string literal with a Spark
cast. Table names accept only safe ASCII components and must include both catalog and
namespace. A map column for which Spark does not allow xxhash64 fails before the procedure; a
canonicalization must be settled and added separately before applying it to such a schema.

Orphan removal is always dry-run by default; a cutoff of at least 72 hours is not safe enough for
writers that run longer than 72 hours, so turn dry-run off only after the operator has checked the write window.
There is no test against the real Iceberg API in this environment; compatibility is not claimed
as validated at runtime. The new schedule has not run, no service was started/stopped,
Lakekeeper/MinIO were not touched and the DAG was not enabled.

**Running on real tables: not yet run — the operator will run it later.**

## On AWS

The DAG [maintenance_iceberg](../../../dags/maintenance_iceberg.py) ran on Compose EC2 against the S3 table
`bronze.raw_shopee_orders_dp1`, run `scheduled__2026-10-27T00:00:00+00:00`.
The [AWS receipt](../../evidence/aws/data-platform-aws-20261027.json) records plan → execute → validate 3/3 success,
`no_op=true`: only the orphan dry-run inventory; before/after both 180 files, 4,689,957 rows, fingerprint unchanged.

The [maintenance job](../../../spark_jobs/maintenance/maintain_iceberg_tables.py) does not merge across daily partition boundaries.
A separate optimization run used [partition evolution + rewrite-all](../../../spark_jobs/maintenance/repartition_small_channel.py),
180 → 6 files, keeping the fingerprint. [AWS storage](../data_storage.md#on-aws) records the measurements and limitations;
there is no dedicated before/after screenshot of AWS compaction yet, and the no-op is not called a successful optimization.
