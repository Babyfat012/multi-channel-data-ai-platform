# Data Platform

The Data Platform normalizes multi-Channel sales data and provides quality-checked, traceable data for analytics, the ML Platform and the LLM Platform.

| Scope | Code / configuration | Documentation |
|---|---|---|
| Simulated sources and data contracts | `data_generator/`, `configs/`, `contracts/` | [Generator](generator_design.md), source contract (archived in git tag `v2-archive`) |
| Batch, lakehouse and warehouse | `spark_jobs/`, `trino/` | [Spark](spark.md), [Silver](silver.md), [Gold](gold.md), [Storage](../data_storage.md) |
| Streaming and CDC | `flink_jobs/` | [Flink](flink.md), [CDC](cdc.md) |
| Orchestration and governance | `dags/` | [Airflow](../pipeline_orchestration.md), [DataHub](../data_governance.md) |
| Deployment | `docker-compose.yml`, `infra/aws/` | [Docker](../docker.md), [AWS](../../shared/aws_deploy.md) |

[Data Platform and ML Platform architecture](../../diagrams/architecture-data.png) · [Evidence](../evidence.md) · Progress (archived in git tag `v2-archive`).

Gold data and the offline feature tables are inputs to the [ML Platform](../../ml-llm-platform/reference/ml_platform.md). The [LLM Platform](../../ml-llm-platform/reference/llm_platform.md) uses the data through tool APIs and reviewed document sources.

[Documentation index](../../README.md) · [Data & AI Platform](../../../README.md)
