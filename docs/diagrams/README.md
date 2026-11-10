# System architecture

The project's two main diagrams:

- **Data Platform**: [Zoomable view](https://babyfat012.github.io/multi-channel-data-ai-platform/docs/diagrams/data-platform-system-architecture.html) ([static image](https://babyfat012.github.io/multi-channel-data-ai-platform/docs/diagrams/data-platform-system-architecture.html?static)) · [Animated SVG](data-platform-system-architecture.svg) · [Static PNG](data-platform-system-architecture-excalidraw.png) · [Excalidraw](data-platform-system-architecture.excalidraw).
- **ML + LLM Platform**: [Zoomable view](https://babyfat012.github.io/multi-channel-data-ai-platform/docs/diagrams/ml-llm-platform-system-architecture.html) ([static image](https://babyfat012.github.io/multi-channel-data-ai-platform/docs/diagrams/ml-llm-platform-system-architecture.html?static)) · [Animated SVG](ml-llm-platform-system-architecture.svg) · [Static PNG](ml-llm-platform-system-architecture-excalidraw.png) · [Excalidraw](ml-llm-platform-system-architecture.excalidraw).

Each white block is one deployment unit (Compose container, Deployment / Pod, Job or managed service); colored frames are functional groups. Arrows follow the direction of data, are labeled, numbered and colored per flow; dashed lines are used only for secondary flows (orchestration, CI/CD, observability, provisioning). The flow legend is at the bottom of each diagram.

The diagrams describe the architecture as configured in the repo; deployment status and evidence are in the [AWS validation status](../shared/aws_validation.md).

## Data Platform

| Group in the diagram | Function | Configuration / docs |
|---|---|---|
| Batch ingestion | Offline generator; skew, evolution, duplicates; source storage | `generator/`, [generator](../data-platform/data_generator.md) |
| Spark | Baseline, skew, schema evolution, dedup; pipeline integration | `spark_jobs/`, [Spark](../data-platform/processing_jobs.md) |
| Kafka / Flink | Online generator, late arrival, duplicates, windows | `flink_jobs/`, [Flink](../data-platform/processing_jobs.md) |
| Lakekeeper / S3 / Iceberg | Catalog, zones, SCD2, labels, features, compaction / partitioning | [Storage](../data-platform/data_storage.md), [Schema](../data-platform/schema_design.md) |
| PostgreSQL | Warehouse dim–fact / indexing | [Warehouse](../data-platform/reference/warehouse_order_lifecycle.md) |
| Airflow DP1–DP4 | Ingest, validate, task dependencies, publish | `dags/`, [Airflow](../data-platform/pipeline_orchestration.md) |
| DataHub | Dataset / DataJob lineage, validation assertions | [DataHub](../data-platform/data_governance.md) |
| Trino | Trino federation | [Trino](../data-platform/trino_federation_and_cdc.md) |
| Debezium | WAL CDC → Kafka | [CDC](../data-platform/trino_federation_and_cdc.md) |
| Infrastructure | Docker / Compose, Dockerfile optimization, Terraform / Ansible | `docker-compose.yml`, `docker-compose.aws.yml`, `infra/aws/` |

## ML + LLM Platform

| Namespace / runtime | Workload / resources | Function |
|---|---|---|
| EC2 · Airflow / stream | `ml_feature_materialize`, `ml_rag_ingest`, Kafka / Flink, 2 PyFlink stream-feature jobs (Flink ML 2.2) | Batch materialization, streaming push to online + offline, RAG pipeline / governance |
| S3 | Offline features, training_lines, registry, RAG raw / chunk / embedding, MLflow artifacts | Offline feature store, dataset / model versioning |
| `feature-store` | Redis, `feature-api` (REST) and `feature-mcp` (MCP), each release 2–3 pods with KEDA, Helm | Online features, async FastAPI / Pydantic, health, REST / MCP in separate pods, rolling update, autoscaling |
| `kubeflow` | KFP pipeline, `mle-drift` | PIT join → split → train → evaluate → register; per-Channel drift monitoring |
| EC2 · MLflow / CLI + ECR | `return-risk` registry, `deploy_return_risk.sh`, image containing the model | Tracking / versioning / production gate / deploy |
| `ml-serving` / `kserve` | `return-risk` InferenceService 2–3 pods with KEDA / KServe controller | ML inference; predictor reads Feast online, autoscaling |
| `rag` | pgvector, PVC `pgdata` | Feast vector search / top-k policy chunks |
| `llm` | `qwen`, `qwen-small`, model PVC | Custom self-hosted GGUF, benchmark, LLM A/B |
| `kagent` | UI, coordinator, data-agent, model-agent, ModelConfig / RemoteMCPServer | Agent tools, A2A orchestration, global model config; KEDA for data-agent / model-agent / coordinator |
| `agentgateway` | agentgateway | LLM / MCP / A2A gateway, API key / PII / A/B routing |
| `ingress` / `cert-manager` | NGINX controller / certificate controller | NLB / HTTPS / basic auth / rate limit, API / UI / ops routes |
| `observability` / `keda` | Prometheus, Grafana, Fluent Bit / Loki, Jaeger / KEDA controller | Metrics / logs / traces, LLM / agent telemetry, autoscaling |
| `agentregistry` / `langfuse` | Agent registry / Langfuse | Publish agents + MCP, prompt versions / production sync |
| `ate-system` / `kagent` | Substrate controllers / SandboxAgent + WorkerPool | Sandbox / gVisor / shared warm worker / golden snapshot |
| EC2 · Jenkins / Vault + IaC | CI pipelines, secret KV, Terraform / Ansible | Test / build / deploy / eval; coverage / EP-BVA / mutation / property-based / Locust; security |

Configuration sources: `infra/k8s/`, `infra/helm/feature-api/`, `ci/jenkins/`, `scripts/setup/`, `ml/`, `serving/`, `rag/`, `feature_repo/`.

Limitations are noted directly below the ML + LLM diagram: the Model Agent's RAG eval is below threshold (3/5); the Coordinator gate only scores routing; stream features use replayed data; drift runs as an on-demand Job and does not retrain automatically. A/B, sandbox and DataHub are enabled by phase. See the validation status page for per-item evidence.

Open and edit `.excalidraw` files with excalidraw.com (File → Open) or the Excalidraw extension in VS Code; re-export the PNG when the diagram changes.
