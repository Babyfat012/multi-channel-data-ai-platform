<!-- Describes v2 metadata from code/contracts/receipts; not yet sent to DataHub in the sandbox. -->
# DataHub — v2 source datasets, jobs and assertions

> **Pipeline v2 (order lifecycle) — designed and tested locally; not the pipeline deployed on AWS.** The pipeline running on AWS is v1 (DP1 → DP4 + ML); see [Data Platform evidence](../evidence.md). Mentions of GCP below are an old plan that was never run; the current cloud target is [AWS](../../shared/aws_deploy.md).

[`scripts/run/publish_order_lifecycle_datahub.py`](../../../scripts/run/publish_order_lifecycle_datahub.py) builds JSON
MetadataChangeProposals (MCPs) with the standard library. By default it only prints offline;
`--dry-run` also prints **every MCP that would be sent**, including schema, lineage and FAIL results.
Only `--publish` loads the DataHub SDK and sends the MCPs to GMS. The v1 script is unchanged.

## Datasets and schemas

The list of Iceberg tables is taken directly via the AST of the jobs `spark_jobs/bronze/ingest_order_lifecycle.py`, `spark_jobs/silver/build_order_lifecycle.py` and `spark_jobs/gold/build_order_lifecycle.py`, without importing Spark:

- One table, `lakehouse.bronze.lifecycle_raw_records`.
- 18 `lakehouse.silver.*` tables: 17 `lifecycle_stg_…` tables (including `lifecycle_stg_observed_evidence` and
  `lifecycle_stg_coverage_validity`) and `quarantine_records`.
- Five `lakehouse.gold.*` tables: `fact_order_lines`, `label_return`,
  `feat_sku_return_rate_90d`, `label_segments`, `label_inputs`. The Gold job has only Silver inputs (no Bronze anymore).

See full names, grain and relationships in the [v2 ERD](../../diagrams/schema-order-lifecycle.md).
Sources are registered separately: 16 file feeds (5 native, 9 operational, coverage,
uncertainties) and PostgreSQL `walmart_lifecycle.orders`. Truth/probe data is not registered.
If `--source-root` has a `manifest.json`, the publisher takes the feed directories from the
`role=source` paths in the manifest; otherwise it uses the directory pattern matching the layout of the job `spark_jobs/bronze/ingest_order_lifecycle.py`:
`<source_root>/dataset_version=*/producer=*/feed=<feed>`.
Use the same `--source-root` identifier across publishes so that different source datasets
are not created just because the mount location changed. URNs use the platforms `file/postgres/iceberg`,
env `PROD`; the v2 namespaces are separate from the v1 `DATASETS` list.

Source schemas come from `contracts/registry.json` and the corresponding JSON Schemas. Without
a manifest, the schema is the union of the paths of the registered versions, with the schema_id in the
field description; that is the source contract, and it does not claim that every field is present in the
run. With a manifest, native feeds use the declared version. For PostgreSQL, the schema describes the
contract payload built by the JDBC query; it is not yet an introspection of the physical DB schema.

With `--receipt`, the jobs `spark_jobs/bronze/ingest_order_lifecycle.py`, `spark_jobs/silver/build_order_lifecycle.py` and `spark_jobs/gold/build_order_lifecycle.py` record the schema of the exact DataFrame written to Iceberg,
with `"schema_file":"schemas.json"` in the receipt. The path is relative to the receipt;
the file has the form
`{"tables":{"lakehouse.gold.lifecycle_fact_order_lines":{"type":"struct","fields":[…]}}}`
(Spark `df.schema.jsonValue()`), keeping names, types, nullability and nested fields.
The helper `spark_jobs/lib/receipts.py` writes a temporary file and then renames it atomically; an empty schema or one with missing
fields is rejected before the file is replaced. Jobs in the same run execute sequentially and merge
schemas by table name in `schemas.json`; Silver also includes the auxiliary table `lifecycle_stg_build_info`.
Each run needs its own receipt directory; when copying receipts, also copy the `schemas.json`
next to them. This is the DataFrame schema used for writing, not a catalog introspection.
Older receipts without `schema_file` can still be read: the dataset description states
**“no schema fields yet”**, and the raw contract is not used to fake a Silver/Gold schema.
Without `--receipt`, the jobs do not output the additional schema/timing.

## Five jobs and lineage edges

Flow `airflow/dp_order_lifecycle/prod`; each DataJob keeps the real task name, with the dependency chain
`check_contracts → ingest_bronze → build_silver → build_gold → validate_gold`.

| Job | Input datasets | Output datasets |
|---|---|---|
| `check_contracts` | No tables: checks the registry file | None |
| `ingest_bronze` | 16 file feeds; if JDBC is enabled, the DB replaces the Walmart file | Shared Bronze |
| `build_silver` | Bronze | All 18 Silver tables |
| `build_gold` | The 4 tables in `SILVER_TABLES` of the job `spark_jobs/gold/build_order_lifecycle.py` (line_lifecycle, native_order_lines, observed_evidence, coverage_validity) — does **not** read Bronze | Five Gold tables |
| `validate_gold` | The same 4 Gold inputs and the five Gold tables written | None |

The PostgreSQL table is always registered, but it only becomes an input when
`--walmart-jdbc walmart_lifecycle.orders` is passed, matching the DAG run Param. The Walmart file is then
still registered as bootstrap, with no input edge to Bronze. The publisher creates conservative dataset
lineage between every input/output of each transforming task; it does not claim
field-level lineage. The validator has inputs and produces no data output.

## Assertions from executed receipts

Pass `--receipt-dir` for a directory with file names as in the DAG, or repeat
`--receipt TASK=PATH` to use receipts with older names. All five receipts are not required; assertions are only created
for checks that actually exist, and checks of tasks that have not run are not filled in.
Each element of `checks` in Silver/reconcile and Gold/validate becomes a separate
assertion; a violation count of 0 → `SUCCESS`, non-zero (including negative) → `FAILURE` (FAIL).
Failed checks are not dropped when publishing. `quarantine` and `info` are observed counts and do not
become failed assertions. Checks must be finite numbers; a missing `run_id` is rejected.

The assertion URN `urn:li:assertion:platform-dp_order_lifecycle-<task>-<check>-v2` is separate from v1.
`assertionRunEvent.runId` and `result.nativeResults.run_id` keep the ID from the receipt,
without replacing it with the ID at publish time. Several independent receipts can carry different run_ids.
Silver assertions are attached to `lifecycle_stg_line_lifecycle` as the representative dataset of the
reconcile group; Gold/Validate are attached to `fact_order_lines`, representing the Gold group. This does not
mean the check only tests the representative table: PIT/coverage checks have multiple inputs, like the job.

With `--receipt`, the jobs `spark_jobs/bronze/ingest_order_lifecycle.py`, `spark_jobs/silver/build_order_lifecycle.py`, `spark_jobs/gold/build_order_lifecycle.py` and `spark_jobs/gold/validate_order_lifecycle.py` record `completed_at` in ISO-8601 UTC with the suffix
`Z`, after writing the tables and finishing the checks. A Gold `failed_before_write` receipt
also has the check completion time but no `schema_file`, because no table was written.
`timestampMillis` prefers `completed_at` and is marked `timestamp_source=completed_at`.
Older receipts without this field still use the file mtime and are marked
`timestamp_source=receipt_mtime`; copying the file can change the mtime, so this is not a
proven Spark completion time. Contracts/Bronze do not output numeric `checks`,
so no assertions are invented for them.

The DAG passes `--receipt <DIRECTORY>/ingest_bronze.json` to the job `spark_jobs/bronze/ingest_order_lifecycle.py` and keeps
the Spark receipt as is on success, including `schema_file` and `completed_at`; the publisher
reads the Bronze schema from the `schemas.json` next to the receipt, as for Silver/Gold. Only when Spark
fails does the operator write a receipt with `status=failed`, `checks_available=false` and then
re-raise the error. A failure receipt does not confirm the Bronze schema or numeric checks.

## Offline commands and real publishing

Dry-run with the Silver receipt already in the repo; no GMS/SDK needed:

```bash
.venv/bin/python scripts/run/publish_order_lifecycle_datahub.py --dry-run \
  --receipt build_silver=docs/evidence/silver-v2-receipt-20261018.json \
  --source-root /data/order_lifecycle --walmart-jdbc walmart_lifecycle.orders > /tmp/datahub-v2-mcps.json

# After the operator has run the DAG and copied the receipts to the host:
.venv/bin/python scripts/run/publish_order_lifecycle_datahub.py --dry-run \
  --receipt-dir out/receipts-v2-run/REPLACE_RUN_ID --source-root /data/order_lifecycle \
  --walmart-jdbc walmart_lifecycle.orders > /tmp/datahub-v2-run-mcps.json
```

The operator checks the JSON and then publishes for real with the existing SDK environment (the token is read from
`DATAHUB_GMS_TOKEN`, never written into the args):

```bash
"$HOME/.local/share/uv/tools/acryl-datahub/bin/python" scripts/run/publish_order_lifecycle_datahub.py \
  --publish --server http://127.0.0.1:8089 --receipt-dir out/receipts-v2-run/REPLACE_RUN_ID \
  --source-root /data/order_lifecycle --walmart-jdbc walmart_lifecycle.orders
```

Use the same arguments for dry-run/publish and keep the receipts unchanged so the MCPs correspond.
The real publish command **has not been run**. **Not measured yet — measure on GCP:**
`bash scripts/run/run_order_lifecycle_live.sh --phase datahub` after the airflow phase
succeeds on a host prepared following the runbook below and the GCP checklist.
There is no confirmation yet of MCP ingestion, UI/Lineage/Quality or the actual assertion count;
these steps have no runtime receipt attached to the repo.

Offline tests use sample receipts in `tmp_path` with sockets blocked; the sample
numbers are not used as evidence of a successful Spark run. Publishing from the DAG is not automated yet, and
MCP ingestion/display on GMS is not proven yet. See the operational limits in [Airflow v2](airflow_order_lifecycle.md).


## Running for real

The operator runs [`scripts/run/run_order_lifecycle_live.sh`](../../../scripts/run/run_order_lifecycle_live.sh) on a host that has
`.env`, Docker Compose, the Spark/Airflow images, DataHub quickstart and the SDK prepared
as for [DataHub v1](../data_governance.md#how-to-reproduce-on-a-machine-with-datahub-quickstart-set-up).
The Python `.venv/bin/python` needs the generator and `aiohttp`; capture needs `google-chrome`.
The script does not install or download tools. By default it reads the Walmart bootstrap file, with no need
to load JDBC; dry-run/publish both use the source `/data/order_lifecycle`, like the DAG.

```bash
# Optionally export PLATFORM_ORDER_LIFECYCLE_DIR=/path/to/fixture before all four commands.
bash scripts/run/run_order_lifecycle_live.sh --phase source
bash scripts/run/run_order_lifecycle_live.sh --phase airflow
bash scripts/run/run_order_lifecycle_live.sh --phase datahub
bash scripts/run/run_order_lifecycle_live.sh --phase capture
# Or run the four phases in order:
# bash scripts/run/run_order_lifecycle_live.sh --phase all
```

The source phase keeps the fixture if a manifest already exists; it refuses to overwrite a non-empty directory
without a manifest. Phase A starts exactly Airflow, the two Spark workers/master, the catalog,
PostgreSQL and MinIO (without running the v1 generator). The default startup timeout is 600s,
DAG 1800s, adjustable with `PLATFORM_V2_START_TIMEOUT`/`PLATFORM_V2_TIMEOUT`.
The runbook picks a new run_id, passes the conf `source_root`/`cutoff`, finds the logical timestamp
through the Airflow metadata, copies the whole receipt/schema directory to
`out/receipts-v2-run/<run_id>/` and then stops phase A. A successful run writes
`out/receipts-v2-run/latest-run-id`; the datahub phase uses this run or the
`PLATFORM_V2_RUN_ID` specified. Do not point the publisher at a parent directory containing several runs.
`PLATFORM_ORDER_LIFECYCLE_CUTOFF` defaults to `2026-06-01T00:00:00.000000Z`.

Phase B checks that all five receipts of the same run are present with no failed checks, stops phase A, starts
quickstart with the existing override, waits for GMS and then runs dry-run/publish with the same arguments.
The dry-run MCPs are saved next to the receipts as `datahub-mcps.json`. The Python SDK defaults to
`$HOME/.local/share/uv/tools/acryl-datahub/bin/python`, changeable with
`DATAHUB_PYTHON`; the token is only read from `DATAHUB_GMS_TOKEN` in the environment.
GMS defaults to `http://127.0.0.1:8089`, the UI to `http://127.0.0.1:9002`.

Estimated phase A RAM is 8–12 GiB (Airflow limited to 6g including a 3g driver, workers
2 × 2560m, master 512m, catalog 256m plus MinIO/PostgreSQL); the total limits
can approach 14 GiB, so stop other workloads. Phase B, the DataHub quickstart, takes about
7–10 GiB, and Chrome for screenshots about 0.5–1 GiB more. These are estimates; the operator measures
the actual usage on the host. Do not run two phases at the same time; the script refuses phase A if
the Compose project `datahub` still has running containers.

Capture uses [`scripts/evidence/capture_order_lifecycle_datahub_evidence.py`](../../../scripts/evidence/capture_order_lifecycle_datahub_evidence.py),
waiting for the dataset, job Lineage and Passing assertions (the number of checks is not fixed at 1):

| Dataset | Lineage | Quality |
|---|---|---|
| `lakehouse.gold.lifecycle_fact_order_lines` | `images/datahub/v2/dp-order-lifecycle-gold-lineage.png` | `images/datahub/v2/dp-order-lifecycle-gold-validation.png` |
| `lakehouse.silver.lifecycle_stg_line_lifecycle` | `images/datahub/v2/dp-order-lifecycle-silver-lineage.png` | `images/datahub/v2/dp-order-lifecycle-silver-validation.png` |

Offline route check: `.venv/bin/python scripts/evidence/capture_order_lifecycle_datahub_evidence.py --print-urls`.
**Not measured yet — measure on GCP:** `bash scripts/run/run_order_lifecycle_live.sh --phase capture`
after the source → airflow → datahub phases above; or run everything with
`bash scripts/run/run_order_lifecycle_live.sh --phase all` on a prepared host.
There is no run_id, timing or real Lineage/Quality screenshot yet; the image paths in the table
are the expected outputs. No fake screenshots are created before the real UI has been run.

On failure, the trap prints the cleanup commands and keeps the receipts/volumes. Use the phase A Compose `stop`
or the quickstart command in docs/data-platform/data_governance.md with `up -d` replaced by `stop` to free
RAM. Do not use `down -v`. Phase B leaves DataHub running for the operator to check the UI;
after capture the operator stops quickstart with that `stop` command. A run that has appended Gold
needs a new run_id when rerun, to avoid clearing/retrying the same generation. A DAG timeout
may leave the job still running; check the logs and stop phase A before moving on to phase B.
