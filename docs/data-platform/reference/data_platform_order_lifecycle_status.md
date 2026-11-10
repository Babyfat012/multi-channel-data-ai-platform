# Data Platform extended-scope progress (v2) — one-page table

> **Pipeline v2 (order lifecycle) — designed and tested locally; not the pipeline deployed on AWS.** The pipeline running on AWS is v1 (DP1 → DP4 + ML); see [Data Platform evidence](../evidence.md). Mentions of GCP below are an old plan that was never run; the current cloud target is [AWS](../../shared/aws_deploy.md).

Evidence includes local results with receipts that are not attached to the repo.
Public artifacts are collected in [evidence](../evidence.md).
Deployment target: **AWS**, region `ap-southeast-1`, Data Platform batch on EC2 + S3 (the operator runs it following the [runbook](../../shared/aws_deploy.md)).
"Local" = checked on a dev machine with temporary fixtures/harnesses/containers; **not** AWS evidence.
A package that passed a local check is not necessarily verified at full scale.

| Component | Documentation | Local | AWS |
|---|---|---|---|
| v2 source fixture, 5 Channels | [silver-v2](silver_order_lifecycle.md) | ✅ fixture 15 Orders / 30 lines | ⏳ |
| v2 source at scale (60k orders ≈ 1.55 GB / 7 minutes) | [source-v2-scale](source_order_lifecycle_scale.md) | ✅ generated 60k; Spark 3/3 on 2,000 orders (200 × 10), reconcile = 0; not yet ≥100 GB | ⏳ |
| Bronze/Silver v2 + reconciliation | [silver-v2](silver_order_lifecycle.md), [Bronze](../../evidence/bronze-v2-jdbc-20261018.json), [Silver](../../evidence/silver-v2-receipt-20261018.json) | ✅ fixture: Bronze 550 = manifest; Silver 19/19 checks = 0 | ⏳ |
| Data contract + quarantine gate | [data contracts](data_contracts.md) | ✅ Spark 32/32 after the identity_path fix; file-based registry in Git | ⏳ |
| Iceberg maintenance | [Iceberg maintenance](iceberg_maintenance.md), [evidence](../../evidence/iceberg-maintenance-local-20261018.json) | ✅ Spark 3/3 on a demo table; not yet run on a schedule or on real business tables | ⏳ real tables |
| Return label v2 (L01–L17) + Gold + PIT features | [gold-v2](gold_order_lifecycle.md) | ✅ Spark fixture 5/5; Gold reads only Silver history; keyless OOM fixed | ⏳ |
| Gold equivalence after refactor | [gold-v2](gold_order_lifecycle.md) | ✅ fact/label/feature diff = 0 on the fixture and the 60-day scale source, before/after the rename; PIT features carry signal after the generator emits weekly coverage (ADR-0013): the 60-day Gold case has **131 lines** with `labeled_count_90d > 0`, Spark matches pure Python | ⏳ |
| Warehouse v2 on PostgreSQL, safe publish | [warehouse-v2](warehouse_order_lifecycle.md) | ✅ real PostgreSQL; warehouse-checks PASS in 116 s before the rename; no conclusion yet on index benefit at scale | ⏳ EXPLAIN at scale |
| DAGs dp_order_lifecycle / dp_order_lifecycle_cdc / dp_order_lifecycle_warehouse + DataHub dry-run + ERD | [airflow-v2](airflow_order_lifecycle.md), [datahub-v2](datahub_order_lifecycle.md), [ERD](../../diagrams/schema-order-lifecycle.md) | ✅ DagBag 10 DAGs / 0 errors; the dry-run is not a real publish | ⏳ |
| Source → Bronze → Silver → Gold → PostgreSQL → EXPLAIN | [e2e-v2-local](e2e_order_lifecycle_local.md) | ✅ small, 45 days × 50 orders, 100 SKUs, 3,631 facts: PASS in 92 s after the fixes and rename; every check = 0 | ⏳ |
| End-to-end chain on the medium source | [e2e-v2-local](e2e_order_lifecycle_local.md) | ⏳ not rerun yet; before the fixes: 500 × 60, 48,874 facts, Silver 2,090 s, gold_build 42 s, gold_validate OOM | ⏳ medium acceptance following the [AWS runbook](../../shared/aws_deploy.md#pipeline); the local harness is kept as a reference |
| Baseline / salted, same input and bag equality | [spark-v2](spark_order_lifecycle.md) | ⏳ not measured, no conclusion on skew reduction | ⏳ measure on S3 following the [AWS runbook](../../shared/aws_deploy.md#do); do not reuse the local MinIO container command as is |
| Real v2 DAG run + DataHub v2 publish/capture | [airflow-v2](airflow_order_lifecycle.md), [datahub-v2](datahub_order_lifecycle.md) | ⏳ DAG run/publish/capture not done; no receipt/Graph/Grid/Lineage/Quality yet | ⏳ pipeline → DataHub → capture following the [AWS runbook](../../shared/aws_deploy.md#pipeline) |
| Terraform + Ansible + S3 overlay | [IaC](../../ml-llm-platform/iac.md), [AWS runbook](../../shared/aws_deploy.md), offline review | ✅ AWS package checked offline; not deployed to the cloud | ⏳ operator |
| Stream v2 Kafka → Flink + oracle | [stream-v2](stream_order_lifecycle.md), [evidence](../../evidence/stream-v2-local-20261019.json) | ✅ harness 2/2 epochs + restart; not HA and not official sales/labels | — |
| CDC v2 on walmart_lifecycle.orders | [cdc-v2](cdc_order_lifecycle.md), [evidence](../../evidence/cdc-v2-local-20261019.json) | ✅ harness insert/update/delete + injected extra rejected | — |
| Inventory + naive/optimized comparison with the same library set | [Docker](../docker.md), [comparison](../../evidence/docker-compare-20261019.json), [local inventory](../../evidence/docker-inventory-local-20261019.json), [archived GCP overlay](../../evidence/docker-inventory-gcp-20261019.json) | ✅ 27 identical packages, 2.39 GB → 698 MB; the overlay inventory was also collected locally | — |

✅ checked within the stated scope · ⏳ waiting to run · 🔨 running · — outside milestone 1 (AWS batch)

## Measurements and limitations

Gold v2 once **ran out of memory (OOM) at small, 3,631 lines** because keyless coverage was duplicated per line.
Fixed; **e2e small PASS in 92 s** after the rename, `gold_build` **7.6 s**, every check **= 0**;
details and EXPLAIN in [e2e local](e2e_order_lifecycle_local.md). Q3 drops **104 → 4** buffers,
Q2 **583 → 583**; Q1 **173 → 2** but **0 rows**, which does not prove an index benefit.
Gold equivalence of fact/label/feature has **diff = 0** on the fixture and the **60-day** scale source,
both before and after the rename; features used to always be **= 0**; after ADR-0013 the 60-day Gold case has **131 lines** with feature > 0, matching pure Python.
**warehouse-checks PASS in 116 s before the rename**; **DagBag 10 DAGs / 0 errors**, including
`dp_order_lifecycle`, `dp_order_lifecycle_cdc`, `dp_order_lifecycle_warehouse` and seven snapshot DAGs that keep their names;
this is only an import check. Unit pytest as confirmed by the operator: **718 passed**.
Medium has not been rerun after the three fixes (keyless Gold, oracle validation grouped by SKU and
Silver `collect_set`); the OOM result in the table is from before the fixes.

The numbers above were recorded from local logs; the receipts in `out/` are not committed,
so only the numbers are given, without receipt links. Do not extrapolate from small/fixture to medium,
≥100 GB or AWS; do not use the warehouse-checks duration as an index improvement figure.

## Still open

- **Measure on AWS:** following the [runbook](../../shared/aws_deploy.md#pipeline), rerun e2e medium after the fixes, measure v2 skew, really run the v2 DAGs +
  send/capture DataHub v2 using the commands in the table above. The operator chose to skip the long
  steps locally; there are no measurements/screenshots yet.
- AWS fixture → scale, pipeline/warehouse receipts and EXPLAIN on the same input following the
  [runbook](../../shared/aws_deploy.md#pipeline); DataHub run locally needs a receipt from the matching AWS generation.
- v2 source at **≥100 GB** on AWS, after small-scale acceptance; do not turn the
  100 GB v1 source figure into v2 evidence or add up volumes across layers.
- Iceberg maintenance on **real business tables**, maintenance/retention schedule and post-run checks;
  Spark 3/3 currently covers only a local demo table.
- Screenshots/schema inventory of the deployed v2 tables, v2 Spark UI screenshots and proof of
  SCD2 dimensions/the v2 dimension–fact relationship are still missing. The local Docker
  inventory and the overlay exist; the overlay does not replace the runtime inventory of the AWS EC2 host.
