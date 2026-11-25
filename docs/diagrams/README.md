# System architecture

[Data platform](architecture-data.png) · [ML + LLM platform](architecture-ml-llm.png) · [CI/CD](architecture-cicd.png) · [Whole platform in one picture](system-architecture.png) (each has an editable `.excalidraw` next to it)

One diagram covers Data + ML + LLM + CI/CD with the EC2, AWS managed services and EKS boundaries. Service internals are left to separate per-service diagrams in `components/` (listed in the [root README](../../README.md#component-diagrams-inside-each-service)). Sources are simulated.

Each box is one service (Compose container, Deployment / Pod, Job or managed service); dashed coloured frames are Kubernetes namespaces or the S3 bucket, grey dashed strips are pipelines and their steps. Each arrow shows the way the data moves (a request and its answer are two arrows); its colour is the business flow and its number, e.g. (B1), gives the order inside that flow. Dotted arrows start or control something without moving data; dashed arrows are CI/CD. The legend is in the Excalidraw file, above the drawing.

The diagram shows the target operating model: pipelines run on schedules and a new model is registered as a challenger that an ML engineer approves. In the current code the schedules exist (nightly `dp_nightly`, starting with the CDC completeness gate; Flink streams Kafka into Bronze continuously; ML refresh on the `GOLD_LABELS` Dataset) but `dp_nightly` is paused on creation, because each run still processes the fixed window set by Variables, not a new day; the challenger → production approval is the `return-risk` Jenkins pipeline. Deployment status and evidence are in the [AWS validation status](../shared/aws_validation.md).

## Data Platform

| Group in the diagram | Function | Configuration / docs |
|---|---|---|
| Batch ingestion | Offline generator; skew, evolution, duplicates; source storage | `generator/`, [generator](../data-platform/data_generator.md) |
| Spark | Baseline, skew, schema evolution, dedup; pipeline integration | `spark_jobs/`, [Spark](../data-platform/processing_jobs.md) |
| Kafka / Flink | Stream to Bronze (dedup status events, Walmart CDC); live new-order counts per 5 min → PostgreSQL `ops.order_counts_window` → Grafana | `flink_jobs/lakehouse/`, `flink_jobs/ops/`, [Flink](../data-platform/processing_jobs.md) |
| Lakekeeper / S3 / Iceberg | Catalog, zones, SCD2, labels, features, compaction / partitioning | [Storage](../data-platform/data_storage.md), [Schema](../data-platform/schema_design.md) |
| PostgreSQL | Warehouse dim–fact / indexing | [Warehouse](../data-platform/data_storage.md) |
| Airflow DP1–DP4 | Ingest, validate, task dependencies, publish | `dags/`, [Airflow](../data-platform/pipeline_orchestration.md) |
| DataHub | Dataset / DataJob lineage, check results of every validate run, Bronze Data Contracts (from CI) | [DataHub](../data-platform/data_governance.md) |
| Trino | Trino federation | [Trino](../data-platform/trino_federation_and_cdc.md) |
| Debezium | WAL CDC → Kafka | [CDC](../data-platform/trino_federation_and_cdc.md) |
| Infrastructure | Docker / Compose, Dockerfile optimization, Terraform / Ansible | `docker-compose.yml`, `docker-compose.aws.yml`, `infra/aws/` |

## ML + LLM Platform

| Namespace / runtime | Workload / resources | Function |
|---|---|---|
| EC2 · Airflow / stream | `ml_feature_materialize`, `ml_rag_ingest`, Kafka / Flink, 2 PyFlink stream-feature jobs (Flink ML 2.2) | Batch materialization, streaming push to online + offline, RAG pipeline / governance |
| S3 | Offline features, training_lines, registry, RAG raw / chunk / embedding, `mlflow/` model files | Offline feature store, dataset / model versioning; KServe loads the production model from `mlflow/` |
| `feature-store` | Redis | Online features (Feast online store) |
| `serving-api` | `feature-api`, `policy-api`, `mcp-server`: one image, three Helm releases, each 2–3 pods with KEDA and its own Pod Identity | Async FastAPI / Pydantic, health, one API per data owner, MCP as a client of the APIs, rolling update, autoscaling |
| `kubeflow` | KFP `return-risk-training` (weekly), `return-risk-monitor` (daily) | PIT join → split → train → evaluate → register challenger; drift vs the production model's train set → train if drift |
| EC2 · MLflow / CLI + ECR | `return-risk` registry, `scripts/ml/deploy_return_risk.py`, code-only predictor image | Tracking / versioning / production gate; deploy points KServe at the `production` model's S3 files |
| `ml-serving` / `kserve` | `return-risk` InferenceService 2–3 pods with KEDA / KServe controller | ML inference behind public `/predict`; storage-initializer loads the model (`STORAGE_URI`), predictor validates order lines, reads Feast online, autoscaling |
| `rag` | pgvector, PVC `pgdata` | Feast vector search / top-k policy chunks |
| `llm` | `qwen` (vLLM × 2), `epp` (llm-d Endpoint Picker), `qwen-small` (vLLM, branch B) | Prefix-cache-aware routing of model A, benchmark, LLM A/B — [LLM inference](../ml-llm-platform/llm_inference_platform.md) |
| `kagent` | UI, coordinator, data-agent, model-agent, ModelConfig / RemoteMCPServer | Agent tools, A2A orchestration, global model config; KEDA for data-agent / model-agent / coordinator |
| `agentgateway` | agentgateway | LLM / MCP / A2A gateway, API key / PII / A/B routing; CI applies it only when every agent/tool is registered |
| `ingress` / `cert-manager` | NGINX controller / certificate controller | NLB / HTTPS / basic auth / rate limit, API / UI / ops routes |
| `observability` / `keda` | Prometheus, Grafana, Fluent Bit / Loki, Jaeger, Langfuse / KEDA controller | Metrics / logs / traces, LLM / agent telemetry, prompt versions / production sync, autoscaling |
| `agentregistry` | Agent registry | Agent + MCP versions (tag = commit, `stable` after eval); the source of what the gateway may expose |
| `ate-system` / `kagent` | Substrate controllers / SandboxAgent + WorkerPool | Sandbox / gVisor / shared warm worker / golden snapshot |
| EC2 · Jenkins / Vault + IaC | CI pipelines, secret KV, Terraform / Ansible | Test / build / deploy / eval; coverage / EP-BVA / mutation / property-based / Locust; security |

Configuration sources: `infra/k8s/`, `infra/helm/serving-api/`, `infra/jenkins/`, `scripts/`, `ml/`, `serving/`, `rag/`, `feature_repo/`.

**No staging environment (decision).** CI/CD has no separate test environment with an approval step before release: a second
Compose stack and set of EKS namespaces on an EC2 machine and node group that are already short of RAM and disk would cost out
of proportion to this project, so the diagram no longer draws one. What stands in for it: job `ci` tests every commit (D2); the
deploy jobs are started by hand (the human approval); each deploy checks itself and rolls back (Compose: last good release;
serving API: `helm --atomic`); the model has its own approval (D11, Jenkins `return-risk`). Known gap: agents are applied with
`kubectl apply` and are not rolled back when their eval fails.

Current limitations: the Model Agent's RAG eval is below threshold (3/5); the Coordinator gate only scores routing; stream features use replayed data; drift sees labeled lines only, so it lags the calendar by ~30 days, and the monitor has not run on EKS yet. A/B, sandbox and DataHub are enabled by phase. See the validation status page for per-item evidence.

Open and edit `.excalidraw` files with excalidraw.com (File → Open) or the Excalidraw extension in VS Code; re-export the PNG (Export image, without the title and legend) when the diagram changes.
