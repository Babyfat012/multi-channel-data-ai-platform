# System architecture theo rubric

Hai sơ đồ chính của dự án:

- **Data Platform**: [PNG](data-platform-system-architecture.png) · [SVG](data-platform-system-architecture.svg) · [HTML](data-platform-system-architecture.html).
- **ML + LLM Platform**: [PNG](ml-llm-platform-system-architecture.png) · [SVG](ml-llm-platform-system-architecture.svg) · [HTML](ml-llm-platform-system-architecture.html).

Hai hình là **deployment topology**, với ranh giới lồng nhau và đối tượng triển khai:

- Data: AWS → VPC → host EC2 → Docker bridge network → Compose service / container → Spark executor / task process. Spark có 1 master + 2 worker container; Flink có JobManager + TaskManager. DataHub có project / network riêng trên cùng host.
- ML + LLM: AWS → VPC / managed EKS data plane → node group 2 worker → các namespace logic → Service → workload controller / ReplicaSet → từng Pod và container. AWS-managed control plane nằm ngoài VPC khách hàng và nối qua cluster API endpoint.
- Mỗi Service ghi type, port → targetPort; Redis / pgvector có NodePort 30379 / 30432. Feature API trên AWS dùng ClusterIP, 2 Pod sau Service. Mũi tên selector chỉ các Pod được Service chọn qua EndpointSlice.
- Deployment chứa các Pod replica minh họa; InferenceService tạo predictor Deployment / Service; Agent CR được kagent controller reconcile thành workload; KFP run tạo task Pods riêng. Số replica theo manifest, không phải ảnh chụp trạng thái live.
- PVC / gp3 / EBS CSI / PV, Secret, ConfigMap, Ingress, ServiceAccount và Pod Identity được ghi trong hoặc cạnh namespace tương ứng. Các namespace phụ trợ có khung riêng.

Node A/B là ký hiệu của hai worker trong sizing bằng chứng AWS, không phải hostname thật. Namespace không thuộc riêng một node; Pod của cùng namespace được scheduler phân bổ lên pool. Manifest có preferred anti-affinity cho Feature API / coordinator / data-agent nên không khẳng định mỗi replica ở một node. Các Pod(s) của chart/upstream chỉ được vẽ đại diện khi repo không pin số replica.

Nguồn đối chiếu rubric là [rubric guide](../rubric-guide.md) và [rubric × cloud audit](../rubric-cloud-audit.md). Các hình mô tả kiến trúc cấu hình trong repo; trạng thái triển khai và bằng chứng nằm trong tài liệu audit.

## Data Platform

| Nhóm trong hình | Rubric / chức năng | Nguồn cấu hình / tài liệu |
|---|---|---|
| Batch ingestion | Generator offline; skew, evolution, duplicate; lưu nguồn | `generator/`, [generator](../generator-design.md) |
| Spark | Baseline, skew, schema evolution, dedup; pipeline integration | `spark_jobs/`, [Spark](../spark.md) |
| Kafka / Flink | Online generator, late arrival, duplicate, window | `flink_jobs/`, [Flink](../flink.md) |
| Lakekeeper / S3 / Iceberg | Catalog, zones, SCD2, label, feature, compaction / partition | [Storage](../storage.md), [Schema](../schema.md) |
| PostgreSQL | Warehouse dim–fact / indexing | [Warehouse](../warehouse-order-lifecycle.md) |
| Airflow DP1–DP4 | Ingest, validate, task dependency, publish | `dags/`, [Airflow](../airflow.md) |
| DataHub | Dataset / DataJob lineage, validation assertions | [DataHub](../datahub.md) |
| Trino | Novel DE-1: federation | [Trino](../trino.md) |
| Debezium | Novel DE-2: WAL CDC → Kafka | [CDC](../cdc.md) |
| Hạ tầng | Docker / Compose, Dockerfile optimization, Terraform / Ansible | `docker-compose.yml`, `docker-compose.aws.yml`, `infra/aws/` |

## ML + LLM Platform

| Namespace / runtime | Workload / tài nguyên | Rubric / chức năng |
|---|---|---|
| EC2 · Airflow / stream | `ml_feature_materialize`, `ml_rag_ingest`, `feast-pusher`, Kafka / Flink | Batch materialize, streaming push online + offline, RAG pipeline / governance |
| S3 | Offline features, training_lines, registry, RAG raw / chunk / embedding, MLflow artifacts | Offline feature store, dataset / model versioning |
| `feature-store` | Redis, Feature API 2 replicas, Helm release | Online features, async FastAPI / Pydantic, health, REST / MCP, rolling update |
| `kubeflow` | KFP pipeline, `mle-drift` | PIT join → split → train → evaluate → register; novel drift theo kênh |
| EC2 · MLflow / CLI + ECR | `return-risk` registry, `deploy_return_risk.sh`, image chứa model | Tracking / versioning / production gate / deploy |
| `ml-serving` / `kserve` | `return-risk` InferenceService / KServe controller | ML inference; predictor đọc Feast online |
| `rag` | pgvector, PVC `pgdata` | Feast vector search / top-k policy chunks |
| `llm` | `qwen`, `qwen-small`, model PVC | Custom self-host GGUF, benchmark, LLM A/B |
| `kagent` | UI, coordinator, data-agent, model-agent, ModelConfig / RemoteMCPServer | Agent tools, A2A orchestration, global model config; KEDA data-agent / coordinator |
| `agentgateway` | agentgateway | LLM / MCP / A2A gateway, API key / PII / routing A/B |
| `ingress` / `cert-manager` | NGINX controller / certificate controller | NLB / HTTPS / basic auth / rate limit, API / UI / ops routes |
| `observability` / `keda` | Prometheus, Grafana, Fluent Bit / Loki, Jaeger / KEDA controller | Metrics / logs / traces, LLM / agent telemetry, autoscale |
| `agentregistry` / `langfuse` | Agent registry / Langfuse | Publish agent + MCP, prompt version / production sync |
| `ate-system` / `kagent` | Substrate controllers / SandboxAgent + WorkerPool | Sandbox / gVisor / shared warm worker / golden snapshot |
| EC2 · Jenkins / Vault + IaC | CI pipelines, secret KV, Terraform / Ansible | Test / build / deploy / eval; coverage / EP-BVA / mutation / property-based / Locust; security |

Nguồn cấu hình: `infra/k8s/`, `infra/helm/feature-api/`, `ci/jenkins/`, `scripts/setup/`, `ml/`, `serving/`, `rag/`, `feature_repo/`.

Các giới hạn được ghi ngay dưới ảnh ML + LLM: model-agent chưa có KEDA; Coordinator CI eval còn timeout; drift là Job theo yêu cầu và không tự retrain. A/B, sandbox và DataHub bật theo pha. Chi tiết proof từng mục xem audit.

## Sinh lại

```bash
python3 scripts/diagrams/system_architecture.py
```

Entry point gọi `scripts/diagrams/deployment_architecture.py`, có kiểm tra resource con nằm đúng khung cha. Script dùng thư viện chuẩn Python và librsvg / Cairo đã có trên máy để xuất PNG; SVG và HTML tự chứa, không tải font hoặc ảnh từ mạng. Hai hình có thể phóng lớn bằng SVG / HTML mà không mất nét.
