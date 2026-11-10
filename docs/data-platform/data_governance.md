# Data Governance on AWS

This page covers lineage (where data comes from and where it goes) and validation (data quality
checks) of the three pipelines DP1/DP2/DP3.

Code / configuration: [AWS manifest](../evidence/aws/datahub-validation-aws-20261029.json).

The results below are from recorded AWS runs; this page does not claim the services are still running.

## On AWS

DataHub 1.7.0.1 runs with Compose on EC2. The metadata publish of six DAGs wrote 22 datasets, 30 jobs and 16
assertions (data quality rules) into DataHub, all PASS. The information is loaded by two scripts:

- The [publisher](../../scripts/run/publish_order_snapshots_datahub.py) declares lineage based on the inputs/outputs of each DAG.
- The [validation builder](../../scripts/evidence/build_order_snapshots_datahub_validation.py) takes the check results from the logs
  of Airflow tasks that succeeded.

The [AWS manifest](../evidence/aws/datahub-validation-aws-20261029.json) records the run_id, task_id, rule and measurements of each check.
The Quality tab screenshots only show that the assertions have a green check. For the run_id and detailed numbers, read the manifest; do not infer them
from the “hours ago” label in the UI.

| DAG / run_id | Tables and purpose | Recorded assertions |
|---|---|---|
| `dp1_bronze` / `manual__2026-10-27T14:58:12+00:00` | Task `ingest_amazon` reads the payload from S3 and writes to `bronze.raw_amazon_orders_dp1`. The other four raw tables hold the source data of each Channel | `validate_amazon`: source row count equals Bronze row count, both 108,248,563; 0 invalid rows. Each Channel has its own check |
| `dp2_silver` / `manual__2026-10-27T16:05:58+00:00` | Task `build_gold_order_lines` merges the five `silver.stg_*_order_lines_dp2` tables into `gold.fact_order_lines_dp2`. Table `gold.dim_amazon_order_dp2` keeps history as SCD2 (stores both the current and the older versions) | `validate_gold_order_lines`: Gold has 227,200,303 rows, equal to Silver; 0 invalid rows, 0 duplicate keys. The dimension table's validator checks the join key with the fact table and that each order has only one current version |
| `dp3_features` / `manual__2026-10-28T00:05:21.167441+00:00` | Task `build_return_features` reads `gold.fact_order_lines_dp2` and writes `gold.feat_order_line_return_dp3`: features at the time the Order was placed | `validate_return_features`: 227,200,303 rows, equal to Gold. Missing, extra, wrong event time, duplicate key and invalid counts are all 0 |

Pipeline code and the role of each task: [Airflow AWS](pipeline_orchestration.md#on-aws).

### DP1: sources and Bronze

![DataHub filtered to AWS S3: four source datasets Amazon, Shopee, Shopify and TikTok Shop under source-exports](../evidence/aws/datahub/s3-sources.png)

![DataHub Bronze Amazon: S3 source through ingest_amazon to raw_amazon_orders_dp1 and downstream tasks](../evidence/aws/datahub/dp1-lineage.png)

![DataHub Quality raw_amazon_orders_dp1: assertion dp1_bronze.validate_amazon with a green check and Passing (1)](../evidence/aws/datahub/dp1-validation.png)

### DP2: Silver and Gold

![DataHub Gold fact: five Bronze branches through Silver tasks, merging into build_gold_order_lines and then fact_order_lines_dp2](../evidence/aws/datahub/dp2-lineage.png)

![DataHub Quality fact_order_lines_dp2: assertion dp2_silver.validate_gold_order_lines with a green check](../evidence/aws/datahub/dp2-validation.png)

### DP3: features

![DataHub: fact_order_lines to build_return_features, feat_order_line_return and then validate_return_features](../evidence/aws/datahub/dp3-lineage.png)

![DataHub Quality feat_order_line_return_dp3: assertion dp3_features.validate_return_features with a green check](../evidence/aws/datahub/dp3-validation.png)

### CDC, labels and warehouse

[CDC DAG](../../dags/dp1_cdc.py), run `manual__2026-10-28T10:51:33.422898+00:00`: task `ingest_walmart_cdc` reads changes
from Kafka and writes them into `bronze.raw_walmart_cdc_v2`. The validator checks 3 offsets; no missing, extra or wrong-payload messages.
[CDC probe and explanation](reference/cdc.md#on-aws).

![DataHub CDC: Kafka orders through ingest_walmart_cdc to raw_walmart_cdc_v2 and validate_walmart_cdc](../evidence/aws/datahub/dp1-cdc-lineage.png)

![DataHub Quality raw_walmart_cdc_v2: assertion dp1_cdc.validate_walmart_cdc with a green check](../evidence/aws/datahub/dp1-cdc-validation.png)

[Label DAG](../../dags/dp3_return_labels.py), run `manual__2026-10-28T16:18:15.667507+00:00`:
task `build_and_validate_return_labels` joins Gold with the answers produced by the generator and writes `gold.fact_return_labels_dp3`
with 179,436,428 rows; per the manifest, 0 duplicate rows and 0 invalid rows. Labels are kept in a separate table, apart from the features,
to avoid leakage (the model accidentally seeing the answer during training).

![DataHub: five Silver branches through Gold fact and the build_and_validate task to fact_return_labels_dp3](../evidence/aws/datahub/dp3-return-labels-lineage.png)

[Warehouse DAG](../../dags/dp4_warehouse.py), run `manual__2026-10-29T08:34:06.602356+00:00`:
task `export_and_index_warehouse` copies 30 days of Gold data into PostgreSQL `warehouse.fact_order_lines_30d_dp2`.
Before the index is created, an assertion reconciles 37,045,741 rows: all five Channels present, row counts matching on both sides.

![DataHub: five Silver tables through Gold fact and export_and_index_warehouse to warehouse.fact_order_lines_30d_dp2](../evidence/aws/datahub/dp4-warehouse-lineage.png)

![DataHub Quality warehouse.fact_order_lines_30d_dp2: assertion dp4_warehouse.export_and_index_warehouse with a green check](../evidence/aws/datahub/dp4-warehouse-validation.png)

Limitations:

- Lineage is declared in the publisher and is not yet derived from the Spark execution plan.
- The metadata and assertions here are the history of the runs in the manifest; this does not claim every later run publishes
  automatically.
- The UI sidebar is broken, but the dataset/Lineage/Quality pages in the screenshots still open.

<details>
<summary>Reference: design, development guide and records from before the AWS deployment</summary>

Local records keep their original environment and do not replace cloud evidence.

## DataHub — lineage and data validation of DP1/DP2/DP3

## Evidence scope

DataHub GMS `v1.7.0.1` received **21 datasets** (22 after adding the CDC v2 table), **29 Airflow jobs** and **15 assertions**
from `scripts/run/publish_order_snapshots_datahub.py`. The six source datasets are the Amazon JSONL,
three small-Channel JSONL sources, the PostgreSQL table `walmart.orders` and the CDC Kafka topic;
the 15 target datasets are the Bronze/Silver/Gold Iceberg tables. The metadata connects source →
Bronze → Silver → Gold → features through the Airflow jobs' inputs/outputs and
dataset lineage edges. The CDC branch connects the Kafka topic to a separate Bronze table.

The assertions read the results of **completed** runs from the Airflow logs in the volume `platform_airflow-data`;
Spark is not rerun at publish time. `scripts/evidence/build_order_snapshots_datahub_validation.py`
only accepts attempts that have the line `Marking task as SUCCESS`, extracts the measurements and timestamps,
checks the PASS condition and then writes
[`evidence/datahub-validation.json`](../evidence/datahub-validation.json). Each assertion
in DataHub keeps the `run_id` and the original metrics of the SUCCESS result. This is an evidence
record of completed runs, not an automated validation monitoring schedule.

| Pipeline | Dataset in the screenshot | Upstream shown in Lineage | Validate SUCCESS |
|---|---|---|---|
| DP1 | `lakehouse.bronze.raw_amazon_orders_dp1` | Amazon JSONL → `ingest_amazon` → Bronze | Source = Bronze **5,827,514**, errors **0**, run `manual__2026-10-13T15:30:09+00:00` |
| DP2 | `lakehouse.gold.fact_order_lines_dp2` | Five Silver tables → `build_gold_order_lines` → Gold | Silver = Gold **10,194,358**, errors **0**, duplicate keys **0**, run `manual__2026-10-14T03:53:52+00:00` |
| DP3 | `lakehouse.gold.feat_order_line_return_dp3` | Gold fact → `build_return_features` → features | Gold = features **10,194,358**, errors **0**, duplicate keys **0**, run `manual__2026-10-14T04:11:15+00:00` |

The screenshots were taken from the `Lineage` and `Quality` tabs of the real UI. The Quality tab shows
`AIRFLOW SPARK VALIDATE` and `1 Passing` for each representative dataset.

### DP1

![Amazon source to Bronze in DataHub Lineage](../images/datahub/dp1-lineage.png)

![DP1 assertion showing Passing in DataHub Quality](../images/datahub/dp1-validation.png)

### DP2

![Five Silver tables to Gold in DataHub Lineage](../images/datahub/dp2-lineage.png)

![DP2 assertion showing Passing in DataHub Quality](../images/datahub/dp2-validation.png)

### DP3

![Gold fact to features in DataHub Lineage](../images/datahub/dp3-lineage.png)

![DP3 assertion showing Passing in DataHub Quality](../images/datahub/dp3-validation.png)

## Rerun evidence

The CDC code (new table `raw_walmart_cdc_v2`, per-run boundary,
source epoch) and the feature validator (key set comparison, event time) have changed. These two
checks were rerun with the current code; the other thirteen checks keep their old runs because
their code did not change.

| Check | New run | Result |
|---|---|---|
| DP1 CDC `lakehouse.bronze.raw_walmart_cdc_v2` | `manual__2026-10-18T05:14:10+00:00` | Kafka = Bronze = unique offsets **4**; missing, extra, wrong payload, errors all **0** ([details](reference/cdc.md#cdc-v2-results)) |
| DP3 `lakehouse.gold.feat_order_line_return_dp3` | `manual__2026-10-18T05:24:29+00:00` | Gold = features **10,194,358**; missing, extra, wrong event time, duplicate keys, errors all **0** |

The first DP3 run of the day (`manual__2026-10-18T05:16:26+00:00`) **failed**
because the executor ran out of heap with only one Spark worker enabled. The `createOrReplace` command was
aborted, so the old feature table stayed intact. With both workers enabled, as in the original configuration, the second
run succeeded. The rerun is still the old DP3 on the old Gold, not yet
features/labels from the v2 source.

The new manifest is [`datahub-validation-v2.json`](../evidence/datahub-validation-v2.json),
created with `--run` and `--cdc-table`; each check records `log_format` (`v1`, `cdc-v2`,
`feature-v2`). The old manifest [`datahub-validation.json`](../evidence/datahub-validation.json)
is unchanged. Checks on v2 tables have their own assertion URN (suffix `-v2`), so the
results of the v1 CDC table remain separate history; the v1 table is marked as legacy in
the dataset description.

The Quality screenshots only show `1 Passing`, not the `run_id`. So the assertion results
were read directly from DataHub's GraphQL and saved in
[`datahub-assertions-v2-20261018.json`](../evidence/datahub-assertions-v2-20261018.json):
the DP3 check has two SUCCESS results, the latest being the run after the code change; the CDC v2 check has
a run after the code change; the CDC v1 check is still the earlier run.

![CDC v2: Kafka topic → ingest_walmart_cdc → raw_walmart_cdc_v2 → validate](../images/datahub/v2/dp1-cdc-lineage.png)

![CDC v2 assertion showing Passing](../images/datahub/v2/dp1-cdc-validation.png)

![Gold fact to features, after the DP3 rerun](../images/datahub/v2/dp3-lineage.png)

![DP3 assertion showing Passing, the latest result being the run after the code change](../images/datahub/v2/dp3-validation.png)

Reproducing the v2 set (once the runs above are in the Airflow volume):

```bash
docker run --rm -v platform_airflow-data:/logs:ro -v "$PWD":/work -w /work \
  python:3.12-slim python scripts/evidence/build_order_snapshots_datahub_validation.py \
  --logs-root /logs/logs --output docs/evidence/datahub-validation-v2.json \
  --run dp1_cdc=manual__2026-10-18T05:14:10+00:00 \
  --run dp3_features=manual__2026-10-18T05:24:29+00:00 \
  --cdc-table lakehouse.bronze.raw_walmart_cdc_v2
"$HOME/.local/share/uv/tools/acryl-datahub/bin/python" \
  scripts/run/publish_order_snapshots_datahub.py --server http://127.0.0.1:8089 \
  --evidence docs/evidence/datahub-validation-v2.json
.venv/bin/python scripts/evidence/capture_order_snapshots_datahub_evidence.py --profile v2
```

## How to reproduce on a machine with DataHub quickstart set up

The `mysql:8.2` image at this pull had a 0-byte `/bin/bash`; the frontend image
`v1.7.0.1` had many 0-byte Play JARs. The file
[`docker/datahub-compose.override.yml`](../../docker/datahub-compose.override.yml)
uses MySQL `8.0` and frontend `v1.6.0.2`, verified to work with GMS
`v1.7.0.1`. This configuration serves the local proof; versions need to be aligned for a long-term
deployment.

```bash
DATAHUB_VERSION=v1.7.0.1 UI_INGESTION_DEFAULT_CLI_VERSION=1.7.0.14 \
DATAHUB_MAPPED_GMS_PORT=8089 DATAHUB_MAPPED_FRONTEND_PORT=9002 \
docker compose --env-file "$HOME/.datahub/quickstart/.local-secrets.env" \
  --profile quickstart -f "$HOME/.datahub/quickstart/docker-compose.yml" \
  -f docker/datahub-compose.override.yml -p datahub up -d

docker run --rm -v platform_airflow-data:/logs:ro -v "$PWD":/work -w /work \
  python:3.12-slim python scripts/evidence/build_order_snapshots_datahub_validation.py \
  --logs-root /logs/logs

"$HOME/.local/share/uv/tools/acryl-datahub/bin/python" \
  scripts/run/publish_order_snapshots_datahub.py --server http://127.0.0.1:8089
.venv/bin/python scripts/evidence/capture_order_snapshots_datahub_evidence.py
```

During this initialization, OpenSearch blocked index creation on its own when the system disk had 13 GB left; after
freeing unused Data & AI Platform images, `cluster.routing.allocation.disk.threshold_enabled` was disabled
for the **single-node** DataHub OpenSearch, with disk usage checked using `df -h /` while
running. If Compose stops at `403 cluster create-index blocked`, then only after
confirming the disk still has enough space, remove the block in the DataHub OpenSearch and rerun the
`docker compose ... up -d` command above:

```bash
docker exec datahub-opensearch-1 curl -sS -X PUT \
  -H 'Content-Type: application/json' \
  'http://localhost:9200/_cluster/settings' \
  -d '{"persistent":{"cluster.routing.allocation.disk.threshold_enabled":false,"cluster.blocks.create_index":null}}'
```

Do not apply that configuration to shared or production environments.
Do not delete Docker volumes to free space: the other large volumes belong to other projects.

UI at `http://localhost:9002`; GMS at `http://localhost:8089`.

</details>

[Section index](README.md) · [Main index](../README.md)
