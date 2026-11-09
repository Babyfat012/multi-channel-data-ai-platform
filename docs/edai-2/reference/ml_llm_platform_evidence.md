# ML Platform / LLM Platform — evidence

> **Trạng thái cuối cùng trên AWS:** [kiểm kê rubric × cloud](../../shared/rubric_cloud_audit.md). Bảng dưới là bản local (đối chứng); các dòng đã có kết quả AWS ghi **Có trên AWS** kèm link.

Các ghi nhận trong bảng dưới thuộc môi trường local (`kind` + Docker Compose). Cột giới hạn phân biệt kết quả chạy, artifact cấu hình và việc chưa kiểm chứng. Bằng chứng AWS mới hơn nằm trong các mục "Trên AWS" / "Trên EKS" của tài liệu thành phần và [kiểm kê rubric × cloud](../../shared/rubric_cloud_audit.md), gồm [CI/CD](../cicd.md#trên-aws), [agent eval](../agent_evaluation.md#trên-aws) và [drift](../drift_monitoring.md#trên-aws-pipeline-a--2026-10-31).

Receipt, log và ảnh gốc giữ nguyên định danh của môi trường đã chạy. Cấu hình hiện hành dùng tiền tố `platform`; các kết quả lịch sử chưa được chạy lại sau đổi tên.

| Thành phần | Ghi nhận local | Evidence / artifact | Giới hạn |
|---|---|---|---|
| LLM inference platform + benchmark | Một phần | llama.cpp (thay llm-d) — [agents](llm_platform_agents.md) | AWS: llama.cpp Qwen 1,5B CPU trên EKS, bench 6/3/4 luồng — [agents](llm_platform_agents.md). llm-d cần GPU nên không dùng |
| Custom LLM model | Một phần | Qwen2.5 GGUF tự host — [agents](llm_platform_agents.md) | Dùng GGUF có sẵn; chưa có bằng chứng fine-tuning riêng |
| agentgateway + security/policy | Đã ghi nhận | API key, rate limit, mcpAuthorization — [agents](llm_platform_agents.md) | Theo phạm vi tài liệu được dẫn |
| Benchmark + tối ưu model server | Đã ghi nhận | config B: req/s 0,475→0,578, p95 9,56→7,0 s — [evidence](../../evidence/llm-benchmark-20261021.jsonl) | Theo phạm vi tài liệu được dẫn |
| Global ModelConfig qua gateway | Đã ghi nhận | `default-model-config` → agentgateway `/v1` — [agents](llm_platform_agents.md) | Theo phạm vi tài liệu được dẫn |
| Agent registry | Đã ghi nhận | agentregistry 0.4.0 + route `registry.` — [gateway](../routing_gateway.md) | ảnh UI |
| RAG pipeline Airflow → Feast | Đã ghi nhận | DAG `ml_rag_ingest` chạy thật — [rag](../rag_pipeline.md) | ảnh Airflow |
| Data governance RAG (lineage) | **Có trên AWS** | bước publish DataHub có sẵn trong DAG | AWS [lineage DataHub](../../evidence/aws/datahub-rag-lineage-20261031.png) — [RAG](../rag_pipeline.md) |
| Materialize offline → online (Airflow) | Đã ghi nhận | DAG `ml_feature_materialize`, tăng dần 2 lần — [feature-store](../feature_store.md) | ảnh Airflow |
| Stream feature → offline + online | Đã ghi nhận | Flink → Kafka → feast-pusher, 72/72 khớp oracle — [stream-feature](../stream_features.md) | ảnh Flink UI |
| Training Pipeline Kubeflow gồm các bước | Đã ghi nhận | 5 bước SUCCEEDED — [training](../training_pipeline.md) | ảnh KFP |
| MLflow version model + data, tag | Đã ghi nhận | v3 `production`, tag `latest`, sha256 data — [training](../training_pipeline.md) | ảnh MLflow |
| KServe model-inside-image | Đã ghi nhận | `return-risk` READY, predict OK — [serving](../ml_inference_service.md) | ảnh pod + curl |
| FastAPI + pydantic + healthcheck | Đã ghi nhận | [web-api](../web_api_mcp.md) | Theo phạm vi tài liệu được dẫn |
| Async concurrent | Đã ghi nhận | `asyncio.gather` theo view — [web-api](../web_api_mcp.md) | Theo phạm vi tài liệu được dẫn |
| MCP tool helm + rolling + fallback | Đã ghi nhận | helm `--atomic` — [web-api](../web_api_mcp.md) | Theo phạm vi tài liệu được dẫn |
| Agent multi-replica + KEDA | Đã ghi nhận | data-agent 2→3→2 — [evidence](../../evidence/keda-data-agent-20261021.txt) | Theo phạm vi tài liệu được dẫn |
| Agent trong Sandbox | **Có trên AWS** | kagent 0.10 chạy Agent trong sandbox qua Agent Substrate (gVisor, valkey ×6, rustfs) — quá nặng cho kind 14 GB; hiện có: Agent không gọi được LLM/tool/Agent khác nếu không qua agentgateway có API key + `mcpAuthorization` — [agents](llm_platform_agents.md) | AWS Agent Substrate trên EKS — [ảnh](../../evidence/aws/substrate-sandbox-agent-ui-20261031.png) |
| Publish agent lên registry | Đã ghi nhận | `publish_agentregistry.sh` — [agents](llm_platform_agents.md) | Theo phạm vi tài liệu được dẫn |
| Agent 2: online feature + predict + RAG | Đã ghi nhận | model-agent gọi KServe `return-risk` + RAG — [agents](llm_platform_agents.md), [serving](../ml_inference_service.md) | ảnh chat |
| MCP gateway + policy | Đã ghi nhận | agentgateway `/mcp` — [agents](llm_platform_agents.md) | Theo phạm vi tài liệu được dẫn |
| A2A gateway + policy | Đã ghi nhận | route theo `x-kagent-host` — [agents](llm_platform_agents.md) | Theo phạm vi tài liệu được dẫn |
| Coordinator multi-replica + scale | Đã ghi nhận | KEDA min 2 / max 3 theo request A2A ở gateway: 2→3→2 pod — [evidence](../../evidence/keda-coordinator-20261021.txt), [`coordinator-keda.yaml`](../../../infra/k8s/agents/coordinator-keda.yaml) | ảnh kagent UI chat |
| Publish coordinator lên registry | Đã ghi nhận | [agents](llm_platform_agents.md) | Theo phạm vi tài liệu được dẫn |
| Warm Up / Agent Fleet (substrate) | **Có trên AWS** | Agent Substrate (kagent `controller.substrate`, WorkerPool gVisor, snapshot/rehydrate agent rảnh) cần ate-system ~2–3 GB RAM — không chạy được cạnh KFP/LLM trên máy 14 GB; local thay bằng scale-to-0 có kiểm soát (KEDA pause, scale qwen 0) | AWS 6 Agent / 1 worker — [ảnh](../../evidence/aws/substrate-warmup-6-agents-1-worker-20261031.png) |
| Unit test coverage | Đã ghi nhận | 85 test, `serving/app.py` 98% (fixture + MagicMock/AsyncMock + httpx MockTransport) — [evidence](../../evidence/serving-coverage-20261021.txt), [ảnh: 86 test, `app.py` 98%, tổng 96%](../../evidence/serving-coverage-20261031.png), [`test_app.py`](../../../serving/tests/test_app.py) | Coverage theo snapshot của báo cáo |
| Equivalence partitioning / BVA | Đã ghi nhận | parametrize biên — `tests/unit/test_serve.py`, `test_agent_eval.py`, [web-api](../web_api_mcp.md) | Theo phạm vi tài liệu được dẫn |
| Mutation testing | Đã ghi nhận | mutmut — [web-api](../web_api_mcp.md) | Theo phạm vi tài liệu được dẫn |
| Property-based (hypothesis) | Đã ghi nhận | model: [`tests/unit/test_model_properties.py`](../../../tests/unit/test_model_properties.py) (dự đoán idempotent, train lại cùng seed ra cùng model, split không lọt tương lai; hypothesis tìm ra ca test một-lớp) + Web API: `test_idempotent` (Hypothesis) — gọi lại cùng request và gửi lặp SKU ra cùng kết quả, [`serving/tests/test_app.py`](../../../serving/tests/test_app.py) | Theo phạm vi tài liệu được dẫn |
| Load test locust (HTML) | Đã ghi nhận | [web-api](../web_api_mcp.md) | Theo phạm vi tài liệu được dẫn |
| CI/CD RAG pipeline | **Có trên AWS** | `ci/jenkins/rag-pipeline.Jenkinsfile`: test → build → **push registry** → deploy → chạy DAG. Image đi qua **registry local** `localhost:5001` (Jenkins push → k8s pull; đã kiểm pod Feature API kéo `localhost:5001/platform-feature-api:<sha>`) — [cicd](../cicd.md). | AWS Jenkins EC2 xanh — [ảnh](../../evidence/aws/jenkins-rag-pipeline-20261031.png) |
| CI/CD MCP + agent (+ Langfuse prompt) | **Có trên AWS** | `feature-api-agents.Jenkinsfile`: test → build → **push registry** → helm `--atomic` → **sync prompt từ Langfuse** → deploy Agent → smoke 4 tool → **Eval agent** (dưới ngưỡng 0,95 ⇒ build đỏ). Langfuse v2 giữ 3 prompt (label production); Agent đọc qua `systemMessageFrom` ConfigMap; đổi prompt v2 trên Langfuse ⇒ Agent nhận sau sync (đã kiểm) — [agents](llm_platform_agents.md#system-prompt-từ-langfuse) | AWS Jenkins EC2 xanh, eval 12/12 — [ảnh](../../evidence/aws/jenkins-feature-api-agents-20261031.png) |
| CI/CD coordinator | Một phần | `coordinator.Jenkinsfile`: validate → sync prompt Langfuse → deploy → smoke A2A qua gateway → **Eval agent** | AWS deploy xanh, eval timeout (LLM CPU) — [CI/CD](../cicd.md) |
| CI/CD stream feature jobs | **Có trên AWS** | `stream-features.Jenkinsfile`: test → build → **push registry** → deploy feast-pusher theo SHA — [stream-feature](../stream_features.md) | AWS Jenkins EC2 xanh — [ảnh](../../evidence/aws/jenkins-stream-features-20261031.png) |
| Metric UI sau gateway | Đã ghi nhận | `metrics.` + `grafana.` — [gateway](../routing_gateway.md) | Theo phạm vi tài liệu được dẫn |
| Log UI sau gateway | Đã ghi nhận | Loki qua Grafana Explore — [observability](../observability.md) | Theo phạm vi tài liệu được dẫn |
| Trace UI sau gateway | Đã ghi nhận | `traces.` (Jaeger) — [gateway](../routing_gateway.md) | Theo phạm vi tài liệu được dẫn |
| UI test agent | Đã ghi nhận | `agent.` (kagent UI) | Theo phạm vi tài liệu được dẫn |
| UI agent registry | Đã ghi nhận | `registry.` | Theo phạm vi tài liệu được dẫn |
| Basic auth + rate limit | Đã ghi nhận | 401/200/429 — [gateway](../routing_gateway.md) | Theo phạm vi tài liệu được dẫn |
| Domain + HTTPS Web API | **Có trên AWS** | sslip.io + CA nội bộ; Let's Encrypt DNS-01 qua DuckDNS đã chuẩn bị, mặc định tắt (`scripts/setup/setup_letsencrypt.sh`) — [gateway](../routing_gateway.md) | AWS: `api.*.sslip.io`, cert Let's Encrypt (HTTP-01) — [gateway](../routing_gateway.md) |
| Terraform cloud services | **Có trên AWS** | [Terraform AWS](../../../infra/aws/terraform/README.md): EC2/VPC/S3/IAM; [IaC](../iac.md) | AWS: EC2/VPC/S3/IAM + EKS, ảnh `terraform plan` — [IaC](../iac.md) |
| Ansible deploy VM | **Có trên AWS** | [Ansible AWS](../../../infra/aws/ansible/README.md), [IaC](../iac.md) (phần Data Platform) | AWS: ảnh `PLAY RECAP` trên EC2 — [IaC](../iac.md) |
| Web API metrics | Đã ghi nhận | feature-api `/metrics`; KServe `return_risk_*` — [serving](../ml_inference_service.md) | Theo phạm vi tài liệu được dẫn |
| CPU/RAM/disk/network | Đã ghi nhận | Prometheus + Grafana — [observability](../observability.md) | Theo phạm vi tài liệu được dẫn |
| Logs | Đã ghi nhận | Loki + Fluent Bit — [observability](../observability.md) | Theo phạm vi tài liệu được dẫn |
| Traces | Đã ghi nhận | agentgateway/kagent OTLP → Jaeger | Theo phạm vi tài liệu được dẫn |
| LLM telemetry (token, TTFT, PII) | Đã ghi nhận | token in/out/tổng mỗi request, thời gian 1 lượt, **TTFT** (`gen_ai_server_time_to_first_token`), **PII**: guardrail agentgateway chặn email/thẻ/SSN (403), `guardrail_checks_total{action}` — dashboard "Data & AI Platform — Agents, LLM, MCP", [observability](../observability.md) | Ảnh AWS: [TTFT/PII](../../evidence/aws/grafana-llm-ttft-pii-20261030.png), [token](../../evidence/aws/grafana-tokens-per-request-20261031.png) |
| Agent telemetry (call agent/tool, lỗi) | Đã ghi nhận | lượt gọi mỗi Agent (`agentgateway_requests_total{route=a2a-*}`), mỗi tool (`agentgateway_mcp_requests_total`), **lỗi mỗi tool** (`mcp_tool_calls_total{tool,outcome}` trong Feature API) — dashboard "Data & AI Platform — Agents, LLM, MCP" | Ảnh AWS: [Agent/MCP](../../evidence/aws/grafana-agents-mcp-gateway-20261030.png), [tool outcome](../../evidence/aws/grafana-mcp-tool-outcome-20261031.png) |
| A/B agent khác model config | Đã ghi nhận | data-agent vs data-agent-b — [ab-test](../ab_testing.md) | Theo phạm vi tài liệu được dẫn |
| A/B hai bản LLM | Đã ghi nhận | `platform-chat` 1:1 — [ab-test](../ab_testing.md) | Theo phạm vi tài liệu được dẫn |
| Vault | Đã ghi nhận | [vault](../security.md) | Theo phạm vi tài liệu được dẫn |
| eval harness agent | Đã ghi nhận | A 100% vs B 91,7%; cổng chạy trong Jenkins (stage `Eval agent`, `scripts/run/eval_agent_ci.sh`) — [agent-eval](../agent_evaluation.md) | ảnh Stage View có stage Eval |
| drift monitoring | Đã ghi nhận | Evidently, Sale event alert — [drift](../drift_monitoring.md) | Theo phạm vi tài liệu được dẫn |

Các dòng “Đã ghi nhận” trỏ tới báo cáo hoặc receipt tại thời điểm chạy, không xác nhận service đang hoạt động. Tài liệu thành phần có lệnh tái lập và phần thiếu ảnh/log; không dùng trạng thái bảng này thay kết quả chạy mới.

## Unit test coverage Web API

`pytest serving/tests --cov=serving`: 86 test pass, `serving/app.py` 98% (4 dòng chưa phủ: `load_store` nạp Feast thật,
và nhánh lỗi HTTP của tool `get_policy_chunks`), tổng 96% > 90%. `locustfile.py` 0% vì là kịch bản load test, không phải code
ứng dụng. Test dùng fixture + `MagicMock`/`AsyncMock` thay Feast, `httpx.MockTransport` thay KServe:
[`serving/tests/test_app.py`](../../../serving/tests/test_app.py).

![pytest coverage serving: 86 passed, app.py 98%, TOTAL 96%](../../evidence/serving-coverage-20261031.png)

## Tái lập và bổ sung evidence

- Chạy lại CI/CD từ revision cần kiểm chứng, lưu Stage View, image digest và kết quả smoke/eval.
- Sandbox và Warm Up đã có ảnh EKS: [SandboxAgent gọi tool](../../evidence/aws/substrate-sandbox-agent-ui-20261031.png), [6 Agent / 1 worker](../../evidence/aws/substrate-warmup-6-agents-1-worker-20261031.png). LLM đang dùng Qwen 1,5B trên CPU; không suy thành proof GPU.
- [AWS EC2 + S3](../../shared/aws_deploy.md) phục vụ Data Platform; ML/LLM chạy trên EKS `platform-apps`. Trạng thái AWS hiện tại xem [kiểm kê](../../shared/rubric_cloud_audit.md); bảng local phía trên giữ nguyên phạm vi lịch sử.

[Mục lục tài liệu](../../README.md)
