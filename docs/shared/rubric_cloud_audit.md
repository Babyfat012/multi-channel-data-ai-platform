# Rubric × cloud — kiểm kê (lần đầu)

Nguồn: rubric coursework EDAI K11 (file Excel của khoá học tại `edai-k11-cw/rubic/`), sheet `de` (100đ) và `mle` (100đ). Nguyên tắc đã chốt:
mọi dòng phải có bằng chứng **triển khai trên AWS**; thiếu là phải làm. Bằng chứng cloud ghi ở
[data-platform-evidence.md](../edai-1/evidence.md) mục "Trên AWS" và các mục AWS/EKS của tài liệu ML/LLM;
[ml-llm-platform-evidence.md](rubric_cloud_audit.md) giữ bảng local lịch sử và dẫn tới kết quả AWS mới hơn.

Ký hiệu: ✅ có bằng chứng AWS · ⚠️ có trên AWS nhưng thiếu phần rubric đòi · ❌ chỉ local / chưa có.
Các dòng ⚠️ về ảnh: chạy trên AWS; ảnh chi tiết là local hoặc chưa có capture tương ứng trên AWS.
Không dùng trạng thái job xanh thay ảnh chi tiết mà cột "Document" yêu cầu.

## Sheet `de`

| Dòng rubric | Điểm | Trạng thái | Việc còn lại |
|---|---|---|---|
| Docker & Compose + optimize Dockerfile | 2+3 | ✅ | Compose chạy trên EC2; đo lại naive 2,4 GB → 698 MB, cùng 27 package ([ảnh EC2](../evidence/aws/docker-naive-vs-opt-ec2-20261031.png)) |
| Generator offline: skew, schema evolution, duplicate, lưu nguồn | 2×4 | ✅ | 100 GB sinh trên EC2, raw S3 + Postgres EC2 |
| Generator online: late arrival, duplicate stream | 2×2 | ✅ | Kafka + producer trên EC2 |
| Spark baseline / skew / schema evolution / other + Spark UI | 2+3+3+3 | ✅ | Chạy trên AWS; skew có Spark UI AWS ([Spark](../edai-1/processing_jobs.md#trên-aws-2026-10-29)). Spark UI AWS của Silver Amazon mở từ event log, thấy hai nhánh schema cũ/mới → `Union` → `Window` dedup, bỏ 3.098.355 bản trùng (1,974%) ([ảnh](../edai-1/processing_jobs.md#trên-aws-2026-10-28)). |
| Spark job tích hợp pipeline | 2 | ✅ | dp1–dp3 trên Airflow EC2 |
| Flink baseline / late arrival / other / window | 2+3+3+2 | ✅ | bốn job Flink chạy liên tục ở trạng thái **RUNNING** (ảnh Flink UI, 6 slot); đầu ra baseline 399.410 dòng, dedup 92.157, late arrival 6.674 dòng đều `event_time <= watermark` ([Flink](../edai-1/processing_jobs.md#flink-trên-aws-2026-10-28)). Lần (bounded, FINISHED) giữ làm số đối chiếu. |
| Lakehouse optimize (compaction/partition) | 3 | ✅ | shopee ngày→tháng, 180→6 file, lọc 30 ngày 8×; ảnh Trino đếm file ở snapshot trước và sau (180 → 6, cùng 4.689.957 dòng) ([storage.md](../edai-1/data_storage.md)) |
| Warehouse indexing | 3 | ✅ | `dp4_warehouse` 37 tr dòng, EXPLAIN 42,6 s → 1,5 s ([storage.md](../edai-1/data_storage.md)) |
| DP1 / DP2 / DP3 ingest + validate | 2×6 | ✅ | Airflow AWS; ảnh Admin → Connections và Variables trên Airflow AWS ([Orchestration](../edai-1/pipeline_orchestration.md)) |
| DataHub lineage + validation DP1/DP2/DP3 | 2×6 | ✅ | DataHub EC2, 6 DAG: 22 dataset, 30 job, 16 assertion |
| Schema: zones, SCD2, feat_, dim–fact, naming | 2×5 | ✅ | Chạy trên AWS; ảnh DBeaver ER Diagram schema Gold có đường nối (virtual foreign key) trên Trino AWS, cùng dbdiagram và DBML. Ảnh danh sách zone/bảng và DESCRIBE SCD2/feature trong [Schema](../edai-1/schema_design.md#trên-aws). |
| Novel idea 1: Trino federation | 10 | ✅ | Trino EC2, Iceberg S3 = Postgres 4.517 dòng ([trino.md](../edai-1/novel_ideas.md)) |
| Novel idea 2: CDC Debezium | 10 | ✅ | Debezium EC2, probe c/u/d |

## Sheet `mle`

Bằng chứng: [LLM và agent](../edai-2/agents.md),
[RAG](../edai-2/rag_pipeline.md#trên-aws), [Web API](../edai-2/web_api_mcp.md#trên-eks),
[Observability](../edai-2/observability.md#trên-eks), [CI/CD](../edai-2/cicd.md#trên-aws),
[eval](../edai-2/agent_evaluation.md#trên-aws), [drift](../edai-2/drift_monitoring.md#trên-aws-pipeline-a--2026-10-31);
ảnh và receipt trong `evidence/aws/`.

| Dòng rubric | Điểm | Trạng thái | Việc còn lại |
|---|---|---|---|
| LLM inference platform + benchmark | 2+2 | ✅ | llama.cpp Qwen 2.5 1,5B trên EKS; bench 6/3/4 luồng (`bench-llm-eks-t*.json`), chọn 3. ảnh triển khai (pod, PVC, log) và **llm-d-benchmark** chạy run-only vào endpoint llama.cpp: 3 mức tải, 42 request, 0 lỗi, bão hòa ở 0,4 req/s ([LLM](../edai-2/llm_inference_platform.md)) |
| Custom LLM model | 2 | ✅ | GGUF tự host trên EKS |
| agentgateway + security/policy | 1 | ✅ | `gateway-smoke-20261030.json` 17/17; **22/22** có bảng allow/deny theo policy (API key 401, PII 403, rate limit 429, tool MCP ngoài danh sách bị chặn, A2A từng Agent) và ảnh cấu hình ConfigMap ([Gateways](../edai-2/agent_gateways.md)) |
| Global model config | 2 | ✅ | `default-model-config` → agentgateway; ảnh `kubectl` ModelConfig và model của từng Agent ([Model Config](../edai-2/global_model_config.md)) |
| Agent registry | 2 | ✅ | agentregistry 0.4.0, 1 MCP + 3 agent (`agent-registry-ui`) |
| RAG pipeline (Airflow) + governance (DataHub) | 2+2 | ✅ | `ml_rag_ingest` 6/6 task xanh trên Airflow EC2 gồm `publish` ([ảnh](../evidence/aws/airflow-ml-rag-ingest-publish-20261031.png)); DataHub EC2 lineage corpus → S3 raw → chunks → embedding → pgvector → validate ([ảnh](../evidence/aws/datahub-rag-lineage-20261031.png)). Đã sửa: value aspect escape ASCII, `DataJobInfo.type` dạng union |
| Materialize offline → online | 2 | ✅ | `ml_feature_materialize` 4/4 |
| Push streaming feature → offline + online | 2 | ⚠️ | 2 job PyFlink `ml-sku-return-24h-online` / `-offline` trên AWS (Flink 2.2, [stream](../edai-2/stream_features.md)): Flink UI, metric, checkpoint; `/features` từ `null` có giá trị; đối chiếu giữa chừng 20.316/20.316 cửa sổ, 1.981/1.981 SKU. **Chưa có số cuối: replay đang chạy.** Dữ liệu phát lại |
| KFP training ≥4 bước | 2 | ✅ | `train-aws-3` 5/5 |
| MLflow version model + data + tag | 2 | ✅ | v1 `production` chạy trên AWS; ảnh Tags `data.*` và Artifacts `data/train`, `data/test` của run `mysterious-flea-253`, script đối chiếu SHA-256 artifact với tag (khớp) và truy ngược model v1 → run ([Training](../edai-2/training_pipeline.md#trên-aws)). Liên kết run MLflow ↔ run KFP dựa vào thời điểm. |
| KServe model-inside-image | 2 | ✅ | predict OK |
| FastAPI + pydantic + healthcheck / async / MCP helm rolling + fallback | 1+1+2 | ✅ | Feature API 2 replica trên AWS, Swagger; probe Liveness/Readiness/Startup, `/healthz` và `/readyz` 200 trong từng pod và trên Swagger; Helm rolling update rồi bản hỏng tự rollback (revision 13 → 14 `failed` → 15 `Rollback to 13`), 763/763 request 200 ([Web API](../edai-2/web_api_mcp.md)) |
| Agent 1 multi-replica + KEDA, Sandbox, publish | 2+1+1 | ✅ | data-agent 2 pod + KEDA 2→3→2 ✅, publish registry ✅; Sandbox Agent Substrate 0.0.9 (gVisor) trên EKS, `data-agent-sandbox` gọi tool `get-sku-features` qua gateway và trả số ([ảnh](../evidence/aws/substrate-sandbox-agent-ui-20261031.png)); model 1,5B chép sai `labeled_count` (83,084 thay 830847), positive/rate đúng |
| Agent 2: online feature + predict + RAG | 8 | ⚠️ | Đủ bộ "tương tự" Agent 1 ([checklist](../edai-2/agents.md#model-agent-đủ-bộ-như-data-agent)): tool/chat trên AWS; KEDA `model-agent` 2 → 3 → 2; Sandbox gVisor; publish registry; MCP qua gateway smoke 24/24 sau khi tách pod `feature-mcp`; KServe `return-risk` và `feature-mcp` 2 → 3 → 2. **Chưa đạt:** eval predict 12/12 nhưng policy 3/5 dưới ngưỡng 0,95, Jenkins #13 đỏ do Qwen 1,5B đôi khi không gọi tool ([Eval](../edai-2/agent_evaluation.md)). |
| MCP gateway / A2A gateway | 2+2 | ✅ | route `/mcp`, A2A theo header; Grafana đếm theo route |
| Coordinator multi-replica + KEDA, publish | 2+2 | ✅ | Chạy trên AWS, đã publish registry; scale 2 → 3 → 2 trên EKS (log, ảnh HPA/pod), [Coordinator](../edai-2/coordinator.md) |
| Warm Up (Agent Substrate) | 2 | ✅ | 6 SandboxAgent Ready trên 1 WorkerPool `kagent-default` (DESIRED/REPLICAS 1), golden snapshot ([ảnh](../evidence/aws/substrate-warmup-6-agents-1-worker-20261031.png)) |
| Unit coverage, EP/BVA, mutation, property-based | 1+2+2+2 | ✅ | Kiểm ở mức code (PR testing), không phụ thuộc nơi chạy; ảnh terminal thật của coverage, EP/BVA, hypothesis ([Validation](../edai-2/validation_verification.md)) |
| Load test locust Web API | 2 | ✅ | Locust → Feature API EKS: 3.004 request, 0 lỗi; chạy lại 3.065 request, 0 lỗi, p50 250 ms, kèm ảnh báo cáo ([Validation](../edai-2/validation_verification.md)) |
| CI/CD RAG / agent / coordinator / stream feature | 2×4 | ⚠️ | Jenkins EC2 → ECR/EKS/Compose: `feature-api-agents` #13 đỏ ở eval RAG 3/5 (#8 xanh, eval agent 12/12), `rag-pipeline` #2, `stream-features` #2 xanh ([ảnh](../evidence/aws/jenkins-feature-api-agents-20261031.png), [rag](../evidence/aws/jenkins-rag-pipeline-20261031.png), [stream](../evidence/aws/jenkins-stream-features-20261031.png)); `coordinator`: `Validate`/`Sync`/`Deploy` xanh nhưng cổng `Eval agent` đạt 12/12 ở #9 nhưng chỉ chấm định tuyến (hai lần đầu đỏ hoặc chưa xong do LLM CPU chậm khi tải chồng; xem [Agent Evaluation](../edai-2/agent_evaluation.md#coordinator-trên-aws-04112026)) |
| NGINX: Grafana, log, trace, agent UI, registry UI | 2×5 | ✅ | Route HTTPS + cert LE: grafana., traces., agent., registry. |
| Basic auth + rate limit | 2 | ✅ | 401 không mật khẩu; rate limit trong gateway smoke |
| Domain + HTTPS Web API | 1 | ✅ | `api.15-134-182-157.sslip.io` cert LE, Swagger qua HTTPS |
| Terraform / Ansible | 1+1 | ✅ | Terraform AWS + Ansible chạy thật |
| Observability: Web API metrics, Prom+Grafana, logs, traces | 1×4 | ✅ | Panel Feature API/KServe, Loki `{namespace="feature-store"}`, Jaeger trace agentgateway; panel thành công/thất bại theo từng Agent ([Observability](../edai-2/observability.md)) |
| LLM telemetry (token, TTFT…) / Agent telemetry | 2+2 | ✅ | Grafana token, TTFT, thời gian/token, lượt gọi Agent + MCP tool |
| A/B agent / A/B LLM | 1+1 | ✅ | dashboard A/B đủ sáu panel có dữ liệu (chia traffic, req/s, p95, token/s, token/request, route A/B agent) ([A/B](../edai-2/ab_testing.md)). EKS: LLM `platform-chat` chia 1:1 Qwen 1,5B/0,5B, Grafana 9,13 vs 8,07 req, p95 ~10 s vs ~80 s ([ảnh](../evidence/aws/grafana-ab-llm-20261031.png), [Prometheus](../evidence/aws/ab-llm-prometheus-20261031.json)); agent cùng 12 SKU: A (1,5B) 12/12 ~10 s/câu, B (0,5B) 4/12 ~5 s/câu ([A](../evidence/aws/agent-eval-data-agent-20261031.jsonl), [B](../evidence/aws/agent-eval-data-agent-b-20261031.jsonl)); route chia `/a2a/data-agent-ab/` 6 câu 2/6 ([jsonl](../evidence/aws/agent-eval-data-agent-ab-20261031.jsonl)) |
| Vault | 1 | ✅ | Vault EC2 (EBS), KV v2, mật khẩu ingress |
| README + docs theo rule | 2 | ✅ | README có sơ đồ AWS Data/ML và LLM/Agent, mục lục, cấu trúc repo và liên kết tài liệu theo rubric; các giới hạn ảnh chi tiết ghi riêng ở dòng ⚠️. |
| Novel idea 1 / 2 | 2+2 | ✅ | ML-N1 Jenkins `feature-api-agents` #8 đạt 12/12 ≥ 0,95 ([JSONL](../evidence/aws/agent-eval-data-agent-20261031.jsonl), [ảnh](../evidence/aws/jenkins-feature-api-agents-20261031.png)); chọn SKU có count dưới 7 chữ số vì PII mask còn che số đếm lớn. ML-N2 Job EKS `mle-drift`, Evidently 0.7.23 + pandas 2.3.3: stable không báo, channel_shift báo `seller_sku` JS 0,2028 > 0,2 ([JSON](../evidence/aws/mle-drift-20261031.json)); chạy lại, kết quả trùng, kèm ảnh báo cáo Evidently từ pod EKS ([Drift](../edai-2/drift_monitoring.md)). |

## Ước lượng (cập nhật)

- Tự đánh giá theo bằng chứng AWS hiện có. Các dòng ⚠️ của lần kiểm kê trước đã được bổ sung ảnh hoặc log AWS (Spark UI Union/Window dedup, Flink chạy liên tục RUNNING và late arrival,
  ERD DBeaver, compaction trước/sau, MLflow hash/artifacts, Helm rolling/rollback, KEDA Model Agent và Coordinator, A/B đủ panel).
- **Còn mở: CI eval Coordinator (2đ, ❌).** Pipeline deploy xanh nhưng cổng `Eval agent` chưa đạt; chưa có build xanh. Nguyên nhân chưa xác định (giả thuyết: câu bị bỏ vẫn chiếm slot LLM, làm chậm các câu sau).
- Điểm yếu cần biết dù có bằng chứng: ca `mcpAuthorization` chỉ ghi được "tool ngoài danh sách bị từ chối" (phản hồi `Unknown tool` không phân biệt gateway hay backend); liên kết MLflow ↔ KFP dựa vào thời điểm;
  feature stream `approved_returns_24h` hiện `null` sau khi nạp lại Redis (chỉ nạp lại batch); số đo A/B đọc bằng mắt từ đồ thị, mẫu nhỏ (40 request); llm-d-benchmark chỉ một lần chạy ngắn, không có khoảng tin cậy; publication batch A/B của Feature Store (DB1/DB2) chưa chạy trên AWS, lần nạp lại dùng luồng incremental cũ.
- EKS context `platform-apps`: **2 node r6i.xlarge** (mỗi node 4 vCPU/32 GB). Cùng EC2 Data Platform `r6i.xlarge`: tổng 12/16 vCPU quota.

## Cấu hình AWS

EC2 Data Platform `r6i.xlarge` (4 vCPU/32 GB) + EKS `platform-apps` 2 node `r6i.xlarge`: 12/16 vCPU quota.
~1,1 USD/giờ khi bật cả hai, ~0,18 USD/giờ khi tắt hết (EKS control plane + EBS); chạy theo đợt, tắt khi nghỉ.
Sandbox dùng Agent Substrate 0.0.9 (khớp kagent 0.10.3), tự đặt runtime gVisor qua node agent của nó
([ví dụ kagent](https://www.kagent.dev/docs/kagent/examples/agent-substrate)); code `scripts/setup/deploy_substrate.sh` + `infra/k8s/agents/sandbox/`.
