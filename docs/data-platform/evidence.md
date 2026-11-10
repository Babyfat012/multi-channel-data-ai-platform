# Data Platform evidence on AWS

This page collects the results of the Data Platform runs on AWS into one table. Each row says which component ran,
what was measured, where the evidence is, and what limitations remain. These are the results of recorded runs; this page
does not claim the services are still running.

Some terms used in the table:

- **Receipt**: a JSON file written by a script right after a run, containing the measurements and identifiers of the run.
- **Bronze / Silver / Gold**: the three layers of the lakehouse. Bronze keeps the raw data; Silver is cleaned and split into
  individual Order Lines (one product row in an order); Gold is the fact table used for analytics and feature building.
- **Channel**: a sales channel (Amazon, Walmart, TikTok Shop, Shopify, Shopee).
- **gap #N**: a known limitation, numbered according to the project's gap list.

## On AWS (cloud)

Environment: Sydney region `ap-southeast-2`, one EC2 instance `c6i.2xlarge` plus S3. Spark has 2 workers with 2 cores each.
S3 read/write access is granted through the instance's IAM role (no access keys).
Shared receipt: [data-platform-aws-20261027.json](../evidence/aws/data-platform-aws-20261027.json).

| Component | Recorded result | Evidence | Limitations |
|---|---|---|---|
| Raw on S3 | 17,393 objects, 141,774,479,087 bytes in total, matching the data on the EC2 disk byte for byte. The Amazon, Walmart and SKU sources alone are 100,012,790,412 bytes, matching the manifest (list of files and sizes) generated locally | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | The three additional Channels (TikTok Shop, Shopify, Shopee) could only be checked against the manifest on EC2 |
| Walmart source (PostgreSQL) | Table `walmart.orders` has 10,949,058 rows, `seller.skus` has 2,000 SKUs. Loading took 443 s | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | |
| Bronze Amazon 100 GB | Read data from S3 via `s3a://` and wrote 108,248,563 rows (180 days) into Bronze in 506.6 s | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | Run manually with `spark-submit`; the run inside the Airflow DAG is the `dp1_bronze` row below |
| Bronze Walmart | Read 10,949,058 rows from PostgreSQL over JDBC. The job compared the row count with the source and found it matching. Took 267.2 s | [Receipt](../evidence/aws/data-platform-aws-20261027.json) | Run manually with `spark-submit` |
| Airflow `dp1_bronze` | All 10/10 tasks succeeded (ingest and validate for 5 Channels), all 180 days, in about 55 minutes. Task `validate_amazon`: the source has 108,248,563 rows, Bronze also has 108,248,563 rows, 0 errors | [Graph / screenshot and explanation](pipeline_orchestration.md#on-aws), [Spark master 2×2 cores / screenshot and explanation](processing_jobs.md#on-aws-2026-10-29) | Data is written first and checked afterwards, not yet following the WAP pattern (write-audit-publish: write to a separate branch and publish only after checks pass). For `dp3_features` see the row below |
| Airflow `dp2_silver` | All 14/14 tasks succeeded. The Summary page shows a duration of 1 hour 58 minutes; this figure excludes extra time from Clear/retry. Silver row counts equal Gold per Channel: amazon 153,849,901, tiktok_shop 33,338,110, shopify 17,782,786, walmart 15,562,675, shopee 6,666,831. These are Order Line counts, so they are larger than the Bronze row counts (one order can have several lines). Gold has 0 invalid rows and 0 duplicate Order Line keys. The Amazon SCD2 table (Slowly Changing Dimension type 2: keeps both the current and the older versions of an order) has 106,109,637 current versions and 1,062,966 historical versions, 0 errors | [Summary / screenshot and explanation](pipeline_orchestration.md#on-aws) | Task `validate_walmart_silver` failed on attempt 1 because the root disk was full; the rerun PASSED. The Summary page only shows "success"; the failure is only visible in the Gantt chart |
| Airflow `dp3_features` | All 2/2 tasks succeeded in 1 hour 12 minutes (build about 8 minutes, validate about 64 minutes). The feature table has 227,200,303 rows, equal to the Gold fact table's 227,200,303 rows (total of the 5 Channels from dp2). No missing rows (missing 0), no extra rows (extra 0), no duplicate keys, no rows with a wrong event time | [Summary / screenshot and explanation](pipeline_orchestration.md#on-aws) | The validate step compares row by row with Gold, so it takes about 8 times as long as the build step |
| Airflow `dp3_walmart_labels` | Built a label lookup of 15,562,675 rows (equal to the Walmart Silver/Gold row count) from the ground truth (answers produced by the generator, seed 42), read from S3. 12,165,007 Order Lines are labeled; 0 duplicates, 0 errors | task log (receipt `airflow_dp3_walmart_labels`) | Run 1 ran out of memory (OOM, exit 137) because the worker requested 3g, above the container's memory limit. Lowering `SPARK_WORKER_MEMORY` to 2560m made it PASS. Rows without a label are undelivered orders |
| Airflow `dp1_cdc` + Debezium on EC2 | Debezium 3.4.3 reads the WAL (PostgreSQL's write-ahead log) with `snapshot.mode=no_data` (no snapshot of existing data), pushes changes into the Kafka topic `platform_cdc.walmart.orders`, and Spark then writes to `bronze.raw_walmart_cdc_v2`. The probe (three test events create/update/delete, abbreviated c/u/d) took 731/452/295 ms respectively from commit to arrival in Kafka. Validate task: Kafka 3, Bronze 3, 3 unique offsets; missing, extra and errors all 0 | [probe](../evidence/aws/cdc-probe-20261028.json), [Validate log / screenshot and explanation](trino_federation_and_cdc.md#on-aws-1), [lineage / screenshot and explanation](data_governance.md#on-aws), receipt `airflow_dp1_cdc` | Only 3 test events; load not measured yet. The source table lacks an index, so the probe is slow (gap #15) |
| Kafka + Flink 1.20.3 on EC2 | The producer finished in 41 s: Amazon 399,410 messages, TikTok 93,554 messages (1,397 duplicates), the same counts as the local run. All 5 Flink jobs FINISHED. The baseline job read 399,410 messages. The dedup job received 93,554 records over the HASH edge into the Deduplicate step and kept 92,157, exactly the number of unique keys (unique_keys). The window job produced 7,555 tumbling windows (consecutive, non-overlapping windows) and 37,870 hopping windows (sliding, overlapping windows). The late job separated 10,166 late messages | [Completed Jobs / screenshot and explanation](processing_jobs.md#on-aws-2026-10-28), [manifest + counts](../evidence/aws/stream-manifest-20261028.json) | The Stream uses the `default.yaml` size, not 100 GB. The Kafka volume is on the root disk. The number of late messages varies between runs (local produced 9,143) |
| DataHub 1.7.0.1 on EC2 | Metadata published from Airflow logs on AWS, covering all 5 DAGs (`--dags dp1_bronze dp1_cdc dp2_silver dp3_features dp3_walmart_labels`, `--raw-root s3a://…/source-exports`): 21 datasets, 29 tasks, 15 assertions (data quality rules) PASS. Lineage (the path data takes) connects S3/PostgreSQL/Kafka to Bronze, Silver, Gold and then to features and labels | [Bronze lineage / screenshot and explanation](data_governance.md#on-aws) | Lineage is declared by hand in the script (gap #3). The UI has broken menus and search (gap #14) |
| Airflow `maintenance_iceberg` | All 3/3 tasks succeeded (plan, execute, validate) on table `bronze.raw_shopee_orders_dp1`. The plan step returned `no_op` (nothing to do): it only inventoried orphan files in dry-run mode (listing via `prefix_listing` over S3FileIO). Before and after are identical: 4,689,957 rows, 180 files, 461,051,515 bytes, 1 snapshot, matching fingerprints (hash of the table content) | [Receipt](../evidence/aws/data-platform-aws-20261027.json) (`airflow_maintenance_iceberg`) | The table has 1 file per daily partition (~2.6 MB/file), so compaction (merging small files) has nothing to merge. Daily partitioning for small Channels is a separate gap |
| Airflow `ml_feature_materialize` (Gold A → Feast) | All 4/4 tasks succeeded, run `manual__2026-10-29T00:30:43+00:00`. `export_sku_daily` and `export_training_lines` write Parquet to `s3://<lakehouse>/feature-store/` (training_lines is a 1% hash-based sample and only uses labels available before the cutoff 2026-06-30). `feast_apply` uses the EC2 IAM role (the log shows `Found credentials from IAM Role: platform-data-platform-vm`). `materialize_incremental` loads `sku_return_daily` into Redis on EKS (NodePort 30379) | [feast_apply log / screenshot and explanation](../ml-llm-platform/feature_store.md#on-aws-1) | The Gantt chart shows 2 fixed failures: `export_training_lines` failed because the `run_id` column exists in both the fact table and the label table; `feast_apply` failed because the AWS overlay was missing the `feature_repo` mount. At the Oct 29 run, the `sku_return_24h` view was still empty; see the update in [stream A on Oct 30](../ml-llm-platform/stream_features.md#on-aws-pipeline-a-2026-10-30) |
| ML on EKS: Kubeflow → MLflow → KServe | Kubeflow Pipelines (KFP) 2.17.2 on EKS: run `train-aws-3` (`0485ebd9…`) completed all 5/5 steps in 10:42. The run reads `training_lines` on S3 through Pod Identity `kubeflow/pipeline-runner`, using the ECR image `platform-train:9d49807-m3b`. MLflow runs on EC2 (artifacts stored in S3, accessed via private IP on port 5050) and registered `return-risk` v1 with alias `production`. Model PR-AUC ≈ 0.25, baseline also ≈ 0.25 (`beats_baseline`=1, lift 2.25, prevalence 0.11, test set of 16,411 rows). KServe v0.21.0 in RawDeployment mode: `return-risk` READY, ECR image `platform-return-risk:v1-9d49807-m4a-55e51c6b`. Calling `/v1/models/return-risk:predict` for SKU `APP-00042` returns score 0.166, `abstain=false`. Online features come from Feast: the registry on S3 is read through Pod Identity `ml-serving/return-risk`, the data is in Redis on EKS | [KFP runs / screenshot and explanation](../ml-llm-platform/training_pipeline.md#on-aws) | There were 4 failed KFP runs before: out of memory (OOM) with the 1% sample; the Feast file store joins every daily snapshot, so it had to be switched to batched calls of 20k; port-forward not ready. The final training sample is 2‰ (359,457 rows). The model beats the baseline by a very narrow margin. The 24h stream view was still empty at training time (see the stream A update). The KFP cache is disabled because EKS does not sign CSRs (certificate signing requests) |
| Airflow `dp4_warehouse` + PostgreSQL index | Copies 30 days of the five Channels into `warehouse.fact_order_lines_30d_dp2`: 37,045,741 rows, 5,513 MB. Row counts per Channel are reconciled against Gold before the index is created. EXPLAIN with the rare SKU `APP-00482`: without an index, a Seq Scan (full table scan) takes 42,567 ms; with an index, a Bitmap Index Scan takes 1,497 ms, and the buffers (data pages) read drop from 690,595 to 4,475. For the common SKU `ELE-01103` (5.4% of rows), the planner still chooses a Seq Scan | [Analysis](data_storage.md#warehouse-on-aws-30-days-five-channels), [DAG log / screenshot and explanation](data_storage.md#on-aws) | The overwrite is not atomic (gap #19). There were 2 earlier failures: missing Postgres credentials, missing `warehouse` schema |
| Trino federation on EC2 | One SQL statement reads Iceberg Gold (on S3, via the IAM role) and the PostgreSQL warehouse at the same time. SKU `APP-00482` over 30 days has 4,517 rows and a total amount of 49,632,312.55 in **both** sources | [trino.md](trino_federation_and_cdc.md#on-aws), [query screenshot / screenshot and explanation](trino_federation_and_cdc.md#on-aws), catalog [`trino/catalog-aws/lakehouse.properties`](../../trino/catalog-aws/lakehouse.properties) | Trino is only turned on for measurements (profile `query`) |
| Lakehouse optimize: partition evolution + compaction | Table `bronze.raw_shopee_orders_dp1` changes its partitioning from daily to monthly and is then fully rewritten (rewrite-all). The file count drops from 180 to 6 (file size from 2.44 MB to 73.13 MB). `count(*)` drops from 1.04 s to 0.29 s; a 30-day filter drops from 7.94 s to 0.97 s. The table still has 4,689,957 rows and an identical fingerprint | [Analysis](data_storage.md#lakehouse-on-aws-merging-small-files-with-partition-evolution), [job](../../spark_jobs/maintenance/repartition_small_channel.py) | Rerunning dp1 brings the table back to daily partitioning, so gap #16 is only partly closed |
| Spark skew handling on AWS (Spark UI) | Reads 20 days of Amazon from S3 with AQE disabled (Adaptive Query Execution: Spark adjusts the plan while running). Write mode `hash`: average 91.1 s (5.6% variation), 20 files of 47.3 MB. Mode `distribution-mode=none`: 71.6 s (21.4% faster) but 309 files of 3.1 MB. In the `hash` write stage, the median task takes 2 ms and processes 0 rows, the largest task takes 12 s with 2,173,164 rows (180 empty tasks) | [spark.md 5g](processing_jobs.md#g-remeasured-on-aws-2026-10-29-ec2-r6ixlarge-4-vcpu-s3a-source), [stage summary / screenshot and explanation](processing_jobs.md#on-aws-2026-10-29) | Keep `hash` as locally: 15 times as many small files is not an acceptable price for 21% of write time |
| DataHub 6 DAGs (dp1 → dp4) | Republished from AWS Airflow logs: **22 datasets, 30 jobs, 16 assertions PASS** ([manifest](../evidence/aws/datahub-validation-aws-20261029.json)). DAG `dp3_return_labels` labeled 179,436,428 Order Lines across five Channels. The `dp4_warehouse` lineage connects Iceberg Gold to PostgreSQL `warehouse.fact_order_lines_30d_dp2`, with per-Channel reconciliation assertions | [Warehouse lineage / screenshot and explanation](data_governance.md#on-aws) | The UI sidebar is broken (gap #14), so pages must be opened by URL directly. Still uses the default account (gap #23) |
| Vault on EC2 + HTTPS Ingress on EKS | Vault 2.1.1 stores data on EBS `/mnt/platform-data/vault` and listens only on loopback port 8200. Initialized and unsealed, KV v2 enabled; UI passwords are generated and stored at `secret/platform/{agent-ui,ops-ui}`. F5 NGINX Ingress 5.6.3 sits behind the public NLB `15.134.182.157` (domain `*.15-134-182-157.sslip.io`), using a real Let's Encrypt cert (issuer YR2, expires 2027-01-28) for `api`, `agent`, `grafana`, `metrics`, `traces`. Checked from a laptop: `http://` is redirected with 301 to `https://`; calls to `grafana`/`metrics` without a password are rejected with 401; `ssl_verify_result = 0` (curl's `%{ssl_verify_result}` variable; 0 means the cert verified successfully) | [Grafana HTTPS / screenshot and explanation](#aws-infrastructure-and-observability), receipt `wave2_ingress_vault_observability` | The first cert issuance required opening the routes for about 1–2 minutes (temporarily removing basic auth and the redirect). Renewal at day 60 for password-protected routes gets 401 (gap #24). Vault must be unsealed by hand after every EC2 boot. The wave2 receipt was written before the Feature API existed; for the API/telemetry measurements of Oct 30 see [Observability EKS](../ml-llm-platform/observability.md#on-eks). This receipt does not measure the NGINX UI rate limit yet |
| Observability on EKS + EC2 alerts | Prometheus, Loki + Fluent Bit, Jaeger and Grafana run on EKS (6 pods Running, Loki uses a `gp3` PVC). Prometheus **actively pulls** metrics from exporters on EC2 over private IPs; the security group (SG) only opens ports 9100/9187 to the cluster's SG. Results: `up` for ec2-node = 1, for ec2-postgres = 1, `pg_up` = 1, disk `/mnt/platform-data` 41.9% used, 1 replication slot. The three alert rules `EC2ExporterDown`, `EC2DataDiskAlmostFull` (> 80%) and `PostgresSlotWalRetained` (> 1 GiB) are all health ok and inactive (not firing) | [Targets / screenshot and explanation](#aws-infrastructure-and-observability), [AWS values](../../infra/k8s/observability/prometheus-values-aws.yaml) | There is no Alertmanager, so alerts are only visible on the Alerts page. The wave2 screenshot only shows Jaeger's own traces; for application telemetry see [Observability EKS](../ml-llm-platform/observability.md#on-eks) |
| EKS `platform-apps` | Kubernetes 1.37, 2 nodes `c6i.xlarge` Ready. EBS CSI and Pod Identity installed; StorageClass `gp3` is the default; a PVC write-then-read test succeeded. Redis for Feast is running; calling Redis from EC2 over NodePort 30379 gets `PONG` from both nodes | [PVC smoke / screenshot and explanation](#aws-infrastructure-and-observability) | The bootstrap milestone (initial setup) only has Redis; for KFP/KServe see the ML on EKS row, for agents/LLM see [Agents EKS](../ml-llm-platform/agents.md) |

## Reading the AWS evidence by topic

The table below points from each topic to the corresponding AWS doc. Each doc embeds screenshots of exactly that topic,
explains the measurement and links to the code. Where a topic is noted as still missing a screenshot, only the receipt
and the limitations are kept; that topic is not considered fully validated.

| Topic | AWS doc |
|---|---|
| Docker | [Docker: runtime and EC2 image size measurement](docker.md#on-aws) |
| Batch generator | [Design](data_generator.md#on-aws-2026-10-27-to-2026-10-28), [batch: volume and characteristics measured on AWS](data_generator.md#on-aws) |
| Stream generator | [Stream: late, duplicate and manifest](data_generator.md#on-aws-1) |
| Spark | [Spark UI and DAG integration](processing_jobs.md#on-aws-2026-10-29), [Silver: AWS Spark UI with Union of old/new schema and Window dedup](processing_jobs.md#aws-spark-ui-union-of-oldnew-schema-and-window-dedup-silver-amazon) |
| Flink | [Baseline, dedup, window, late arrival; four jobs running continuously in RUNNING](processing_jobs.md#flink-baseline-late-arrival-dedup-and-window); no AWS Flink UI screenshot with the watermark operator opened yet |
| Storage | [Compaction receipt and embedded EXPLAIN](data_storage.md#on-aws), [maintenance no-op](reference/iceberg_maintenance.md#on-aws) |
| Orchestration | [Airflow DP1/DP2/DP3 and each component](pipeline_orchestration.md#on-aws) |
| Governance | [DataHub tables, lineage, assertions and run_id](data_governance.md#on-aws) |
| Schema | [Schema: zones, DESCRIBE SCD2/feature](schema_design.md#on-aws), [DBeaver ER screenshot with connecting lines on Trino on AWS](schema_design.md#dbeaver-screenshots-connected-to-trino-runtime), [Gold validation](reference/gold.md#on-aws), [design relationships](reference/gold_design.md#on-aws) |
| Trino federation | [SQL over two catalogs and UI FINISHED](trino_federation_and_cdc.md#on-aws) |
| WAL CDC to Kafka | [Probe c/u/d, validator and lineage](trino_federation_and_cdc.md#on-aws-1) |
| Feast | [Materialize DAG and IAM role](../ml-llm-platform/feature_store.md#on-aws-1) |
| Training/versioning | [KFP 5 steps, MLflow model v1, Tags/Artifacts screenshot and train/test SHA-256 cross-check](../ml-llm-platform/training_pipeline.md#on-aws) |

### AWS infrastructure and observability

The screenshots below add environment evidence to the summary table above:

- A PVC on EKS can be written and read; EC2 can reach Redis.
- The Grafana and Jaeger UIs open over HTTPS.
- Prometheus pulls metrics from the exporters on EC2.

Related configuration: [StorageClass gp3](../../infra/k8s/storage-aws.yaml) and
[Prometheus AWS](../../infra/k8s/observability/prometheus-values-aws.yaml).

Note when reading the screenshots: the Jaeger screenshot only has traces of the jaeger service itself and does not replace application traces. The
Alerts screenshot only shows the rules as inactive; it does not prove the system can deliver notifications. For newer telemetry see
[Observability EKS](../ml-llm-platform/observability.md#on-eks).

![kubectl: Pod Identity and EBS CSI rolled out successfully, gp3 default, pvc-smoke Bound and the log reading back the string ok](../evidence/aws/eks-ebs-gp3-pvc-smoke-20261027.png)

![Session Manager EC2: redis-cli to both node addresses on port 30379 returns PONG](../evidence/aws/ec2-to-eks-redis-ping-20261028.png)

![Grafana Dashboards page: the PLATFORM folder has A/B LLM + Agent and Agents, LLM, MCP](../evidence/aws/grafana-dashboards-https-20261030.png)

![Jaeger Search: service jaeger, 10 traces and the api/services, GET spans](../evidence/aws/jaeger-search-https-20261030.png)

![Prometheus Target health: ec2-node and ec2-postgres both UP, 1/1 per pool](../evidence/aws/prometheus-targets-ec2-20261030.png)

![Prometheus Alerts: EC2ExporterDown, EC2DataDiskAlmostFull and PostgresSlotWalRetained in the INACTIVE group (3)](../evidence/aws/prometheus-alerts-ec2-20261030.png)

<details>
<summary>Reference: design, development guide and records from before the AWS deployment</summary>

Local records keep their original environment and do not replace cloud evidence.

## Data Platform — evidence

AWS evidence comes first; local evidence is kept below for comparison. Each measurement has its own data scope and environment; do not extrapolate from fixtures to full scale or from local to AWS.

## Local

Original receipts, logs and screenshots keep their content, timestamps and environment identifiers. Resource names in historical artifacts may differ from the current configuration, which uses the `platform` prefix; renaming the configuration is not the same as rerunning the measurement.

### Data sources and processing

| Component | Recorded result | Evidence / measurement description | Limitations |
|---|---|---|---|
| Docker | Comparison with the same 27 packages: naive 2.39 GB → optimized 698 MB | [Image comparison](../evidence/docker-compare-20261019.json), [local inventory](../evidence/docker-inventory-local-20261019.json), [method](docker.md) | The historical GCP overlay inventory was also collected locally; it does not prove a cloud deployment |
| Batch generator v1 | 100,012,790,412 raw bytes from Amazon, Walmart and SKU; duplicates 1.98% / 1.97% | [Manifest](../evidence/batch-100gb-manifest.json), [batch report](data_generator.md) | The other three Channels use small sources; not 100 GB v2 |
| Stream generator v1 | 492,964 messages; late arrival, out-of-order, duplicates and bursts | [Stream report](data_generator.md) | Small configuration scope, separate from the 100 GB batch |
| Lifecycle source v2 | 60,000 Orders, 97,890 lines, 1,546,066,142 bytes; Spark reconcile on 2,000 Orders | [Scale](reference/source_order_lifecycle_scale.md), [fixture](reference/source_order_lifecycle_fixtures.md) | Has not reached 100 GB v2 |
| Bronze v2 | 550 records: 547 files + 3 JDBC, matching the manifest | [Bronze receipt](../evidence/bronze-v2-jdbc-20261018.json) | Local fixture |
| Silver v2 | 19/19 checks with zero discrepancy; keeps revisions and blocks conflicts | [Silver receipt](../evidence/silver-v2-receipt-20261018.json), [Silver processing](reference/silver_order_lifecycle.md) | The clean receipt has duplicate_rows_dropped=0; it does not replace the v1 duplicate measurement |
| Spark v1 | Baseline/AQE, skew, schema evolution, dedup; execution 3: 8,449,302 → 8,314,312 rows | [Spark report](processing_jobs.md), [event log and UI screenshots](../evidence/spark-ui/README.md) | The runs have different data windows; do not treat them as a v2 benchmark |
| Spark v2 | Baseline/salted workloads and bag-equality checks exist | [Workload and protocol](reference/spark_order_lifecycle.md) | No v2 baseline/salted performance measurement yet |
| Flink v1 | Watermark, late arrival, keyed dedup, tumbling/sliding windows | [Flink report](processing_jobs.md), [state metrics](../evidence/flink-dedup-state.json) | Results are tied to the job/offset recorded in the report |
| Stream lifecycle v2 | Two epochs, checkpoint restart/restore; kept 15,261 matches the oracle | [Stream receipt](../evidence/stream-v2-local-20261019.json), [protocol](reference/stream_order_lifecycle.md) | Bounded event counting; not HA nor official business figures |
| CDC lifecycle v2 | Insert/update/delete match the oracle; the extra case is blocked | [CDC receipt](../evidence/cdc-v2-local-20261019.json), [protocol](reference/cdc_order_lifecycle.md) | Local harness; does not yet prove a live v2 DAG |

### Storage, orchestration and governance

| Component | Recorded result | Evidence / measurement description | Limitations |
|---|---|---|---|
| Iceberg v1 | Compaction 187 → 181 files; seven-day query median 10.348 → 9.405 s | [Storage](data_storage.md) | Per the recorded table and run |
| Warehouse v1 | 1,875,607 Walmart rows; one EXPLAIN pass 51.611 → 1.134 ms after indexing | [Storage](data_storage.md) | Not a measurement over the full five-Channel Gold |
| Federation | Gold and warehouse for the same SKU: 211 rows, net 12,038.74 | [SQL and results](trino_federation_and_cdc.md), [cross-check](reference/data_integration.md) | One Walmart SKU; does not show that cross-source queries are always faster |
| Airflow v1 | DP1/DP2/DP3 have Graph/Grid/logs; runtime configuration smoke succeeded 2/2 tasks | [Airflow](pipeline_orchestration.md), [runtime receipt](../evidence/airflow-runtime-20261015.json) | The configuration smoke does not write business tables |
| DataHub v1 | Lineage and assertions extracted from the Validate run | [DataHub](data_governance.md), [manifest](../evidence/datahub-validation.json) | Historical assertions; does not confirm every later run publishes automatically |
| Airflow / DataHub v2 | DAGs and metadata dry-run available | [Airflow v2](reference/airflow_order_lifecycle.md), [DataHub v2](reference/datahub_order_lifecycle.md) | No live v2 DAG receipt nor matching lineage/quality screenshots yet |
| Data contracts | Infer/diff/gate with Python fixtures; Spark check description 32/32 local | [Fixture receipt](../evidence/contracts-validation-20261018.json), [data contracts](reference/data_contracts.md) | The Python receipt is not Spark evidence |
| Iceberg maintenance | Spark 3/3 on a demo table; checks compact/zorder, data and tags | [Maintenance receipt](../evidence/iceberg-maintenance-local-20261018.json), [mechanism](reference/iceberg_maintenance.md) | No maintenance schedule nor measurement on real business tables yet |

Gold/e2e results whose logs are kept only in `out/` are recorded separately in [lifecycle status](reference/data_platform_order_lifecycle_status.md), [Gold](reference/gold_order_lifecycle.md) and [local e2e](reference/e2e_order_lifecycle_local.md). Those figures are historical records with limited public verifiability and are not to be treated as receipts attached to the repo.

## Missing verification

- Medium e2e after the OOM fixes; a v2 skew measurement with the same input.
- Live lifecycle DAG, DataHub publish/capture from the same generation, and an inventory of the deployed schema.
- 100 GB v2 data, EXPLAIN at the corresponding scale, and maintenance of real business tables.
- The AWS runtime has receipts and screenshots per the table above. The [EC2 + S3 runbook](../shared/aws_deploy.md) is a reproduction guide; Terraform validate or local inventory alone does not replace runtime evidence.

[Docs index](../README.md)

</details>

[Section index](README.md) · [Main index](../README.md)
