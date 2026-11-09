# System architecture theo rubric

Hai sơ đồ chính của dự án:

- **Data Platform**: [Xem phóng to](https://babyfat012.github.io/multi-channel-data-ai-platform/docs/diagrams/data-platform-system-architecture.html) ([ảnh tĩnh](https://babyfat012.github.io/multi-channel-data-ai-platform/docs/diagrams/data-platform-system-architecture.html?static)) · [SVG động](data-platform-system-architecture.svg) · [PNG tĩnh](data-platform-system-architecture-excalidraw.png) · [Excalidraw](data-platform-system-architecture.excalidraw).
- **ML + LLM Platform**: [Xem phóng to](https://babyfat012.github.io/multi-channel-data-ai-platform/docs/diagrams/ml-llm-platform-system-architecture.html) ([ảnh tĩnh](https://babyfat012.github.io/multi-channel-data-ai-platform/docs/diagrams/ml-llm-platform-system-architecture.html?static)) · [SVG động](ml-llm-platform-system-architecture.svg) · [PNG tĩnh](ml-llm-platform-system-architecture-excalidraw.png) · [Excalidraw](ml-llm-platform-system-architecture.excalidraw).

Mỗi khối trắng là một đơn vị triển khai (Compose container, Deployment / Pod, Job hoặc managed service); khung màu là nhóm chức năng. Mũi tên đi theo chiều dữ liệu, có mô tả, đánh số và tô màu riêng cho từng luồng; nét đứt chỉ dùng cho luồng phụ (điều phối, CI/CD, observability, provisioning). Chú giải luồng nằm cuối mỗi hình.

Nguồn đối chiếu rubric là [rubric guide](../shared/rubric_guide.md) và [rubric × cloud audit](../shared/rubric_cloud_audit.md). Các hình mô tả kiến trúc cấu hình trong repo; trạng thái triển khai và bằng chứng nằm trong tài liệu audit.

## Data Platform

| Nhóm trong hình | Rubric / chức năng | Nguồn cấu hình / tài liệu |
|---|---|---|
| Batch ingestion | Generator offline; skew, evolution, duplicate; lưu nguồn | `generator/`, [generator](../edai-1/data_generator.md) |
| Spark | Baseline, skew, schema evolution, dedup; pipeline integration | `spark_jobs/`, [Spark](../edai-1/processing_jobs.md) |
| Kafka / Flink | Online generator, late arrival, duplicate, window | `flink_jobs/`, [Flink](../edai-1/processing_jobs.md) |
| Lakekeeper / S3 / Iceberg | Catalog, zones, SCD2, label, feature, compaction / partition | [Storage](../edai-1/data_storage.md), [Schema](../edai-1/schema_design.md) |
| PostgreSQL | Warehouse dim–fact / indexing | [Warehouse](../edai-1/reference/warehouse_order_lifecycle.md) |
| Airflow DP1–DP4 | Ingest, validate, task dependency, publish | `dags/`, [Airflow](../edai-1/pipeline_orchestration.md) |
| DataHub | Dataset / DataJob lineage, validation assertions | [DataHub](../edai-1/data_governance.md) |
| Trino | Novel DE-1: federation | [Trino](../edai-1/novel_ideas.md) |
| Debezium | Novel DE-2: WAL CDC → Kafka | [CDC](../edai-1/novel_ideas.md) |
| Hạ tầng | Docker / Compose, Dockerfile optimization, Terraform / Ansible | `docker-compose.yml`, `docker-compose.aws.yml`, `infra/aws/` |

## ML + LLM Platform

| Namespace / runtime | Workload / tài nguyên | Rubric / chức năng |
|---|---|---|
| EC2 · Airflow / stream | `ml_feature_materialize`, `ml_rag_ingest`, Kafka / Flink, 2 job PyFlink stream feature (Flink ML 2.2) | Batch materialize, streaming push online + offline, RAG pipeline / governance |
| S3 | Offline features, training_lines, registry, RAG raw / chunk / embedding, MLflow artifacts | Offline feature store, dataset / model versioning |
| `feature-store` | Redis, `feature-api` (REST) và `feature-mcp` (MCP) mỗi release 2–3 pod KEDA, Helm | Online features, async FastAPI / Pydantic, health, REST / MCP tách pod, rolling update, autoscale |
| `kubeflow` | KFP pipeline, `mle-drift` | PIT join → split → train → evaluate → register; novel drift theo kênh |
| EC2 · MLflow / CLI + ECR | `return-risk` registry, `deploy_return_risk.sh`, image chứa model | Tracking / versioning / production gate / deploy |
| `ml-serving` / `kserve` | `return-risk` InferenceService 2–3 pod KEDA / KServe controller | ML inference; predictor đọc Feast online, autoscale |
| `rag` | pgvector, PVC `pgdata` | Feast vector search / top-k policy chunks |
| `llm` | `qwen`, `qwen-small`, model PVC | Custom self-host GGUF, benchmark, LLM A/B |
| `kagent` | UI, coordinator, data-agent, model-agent, ModelConfig / RemoteMCPServer | Agent tools, A2A orchestration, global model config; KEDA data-agent / model-agent / coordinator |
| `agentgateway` | agentgateway | LLM / MCP / A2A gateway, API key / PII / routing A/B |
| `ingress` / `cert-manager` | NGINX controller / certificate controller | NLB / HTTPS / basic auth / rate limit, API / UI / ops routes |
| `observability` / `keda` | Prometheus, Grafana, Fluent Bit / Loki, Jaeger / KEDA controller | Metrics / logs / traces, LLM / agent telemetry, autoscale |
| `agentregistry` / `langfuse` | Agent registry / Langfuse | Publish agent + MCP, prompt version / production sync |
| `ate-system` / `kagent` | Substrate controllers / SandboxAgent + WorkerPool | Sandbox / gVisor / shared warm worker / golden snapshot |
| EC2 · Jenkins / Vault + IaC | CI pipelines, secret KV, Terraform / Ansible | Test / build / deploy / eval; coverage / EP-BVA / mutation / property-based / Locust; security |

Nguồn cấu hình: `infra/k8s/`, `infra/helm/feature-api/`, `ci/jenkins/`, `scripts/setup/`, `ml/`, `serving/`, `rag/`, `feature_repo/`.

Các giới hạn được ghi ngay dưới ảnh ML + LLM: eval RAG của Model Agent chưa đạt ngưỡng (3/5); cổng Coordinator chỉ chấm định tuyến; stream feature dùng dữ liệu phát lại; drift là Job theo yêu cầu và không tự retrain. A/B, sandbox và DataHub bật theo pha. Chi tiết proof từng mục xem audit.

Mở và chỉnh file `.excalidraw` bằng excalidraw.com (File → Open) hoặc extension Excalidraw trong VS Code; xuất lại PNG khi đổi hình.
