# ML Platform / LLM Platform — evidence

> **Final status on AWS:** [AWS validation status](../../shared/aws_validation.md). The table below is the local version (for comparison); rows that have AWS results are marked **On AWS** with a link.

The records in the table below come from the local environment (`kind` + Docker Compose). The limitations column distinguishes run results, configuration artifacts and things not yet verified. Newer AWS evidence lives in the "On AWS" / "On EKS" sections of the component docs and in the [AWS validation status](../../shared/aws_validation.md), including [CI/CD](../cicd.md#on-aws), [agent eval](../agent_evaluation.md#on-aws) and [drift](../drift_monitoring.md#on-aws-pipeline-a-2026-10-31).

Receipts, logs and original screenshots keep the identifiers of the environment they ran in. The current configuration uses the `platform` prefix; historical results have not been rerun since the rename.

| Component | Local record | Evidence / artifact | Limitations |
|---|---|---|---|
| LLM inference platform + benchmark | Partial | llama.cpp (instead of llm-d) — [agents](llm_platform_agents.md) | AWS: llama.cpp Qwen 1.5B CPU on EKS, benchmark at 6/3/4 threads — [agents](llm_platform_agents.md). llm-d needs a GPU, so it is not used |
| Custom LLM model | Partial | Self-hosted Qwen2.5 GGUF — [agents](llm_platform_agents.md) | Uses an off-the-shelf GGUF; no evidence of custom fine-tuning yet |
| agentgateway + security/policy | Recorded | API key, rate limit, mcpAuthorization — [agents](llm_platform_agents.md) | Within the scope of the linked doc |
| Benchmark + model server tuning | Recorded | config B: req/s 0.475→0.578, p95 9.56→7.0 s — [evidence](../../evidence/llm-benchmark-20261021.jsonl) | Within the scope of the linked doc |
| Global ModelConfig through the gateway | Recorded | `default-model-config` → agentgateway `/v1` — [agents](llm_platform_agents.md) | Within the scope of the linked doc |
| Agent registry | Recorded | agentregistry 0.4.0 + route `registry.` — [gateway](../routing_gateway.md) | UI screenshot |
| RAG pipeline Airflow → Feast | Recorded | DAG `ml_rag_ingest` actually ran — [rag](../rag_pipeline.md) | Airflow screenshot |
| RAG data governance (lineage) | **On AWS** | DataHub publish step built into the DAG | AWS [DataHub lineage](../../evidence/aws/datahub-rag-lineage-20261031.png) — [RAG](../rag_pipeline.md) |
| Materialize offline → online (Airflow) | Recorded | DAG `ml_feature_materialize`, incremental twice — [feature-store](../feature_store.md) | Airflow screenshot |
| Stream features → offline + online | Recorded | Flink → Kafka → feast-pusher, 72/72 match the oracle — [stream-feature](../stream_features.md) | Flink UI screenshot |
| Kubeflow Training Pipeline with steps | Recorded | 5 steps SUCCEEDED — [training](../training_pipeline.md) | KFP screenshot |
| MLflow model + data versioning, tags | Recorded | v3 `production`, tag `latest`, data sha256 — [training](../training_pipeline.md) | MLflow screenshot |
| KServe model-inside-image | Recorded | `return-risk` READY, predict OK — [serving](../ml_inference_service.md) | pod screenshot + curl |
| FastAPI + pydantic + healthcheck | Recorded | [web-api](../web_api_mcp.md) | Within the scope of the linked doc |
| Async concurrency | Recorded | `asyncio.gather` per view — [web-api](../web_api_mcp.md) | Within the scope of the linked doc |
| MCP tool helm + rolling + fallback | Recorded | helm `--atomic` — [web-api](../web_api_mcp.md) | Within the scope of the linked doc |
| Agent multi-replica + KEDA | Recorded | data-agent 2→3→2 — [evidence](../../evidence/keda-data-agent-20261021.txt) | Within the scope of the linked doc |
| Agent in a Sandbox | **On AWS** | kagent 0.10 runs Agents in a sandbox through Agent Substrate (gVisor, valkey ×6, rustfs) — too heavy for 14 GB kind; what exists locally: an Agent cannot call the LLM, tools or other Agents except through agentgateway with an API key + `mcpAuthorization` — [agents](llm_platform_agents.md) | AWS Agent Substrate on EKS — [screenshot](../../evidence/aws/substrate-sandbox-agent-ui-20261031.png) |
| Publish agent to the registry | Recorded | `publish_agentregistry.sh` — [agents](llm_platform_agents.md) | Within the scope of the linked doc |
| Agent 2: online features + predict + RAG | Recorded | model-agent calls KServe `return-risk` + RAG — [agents](llm_platform_agents.md), [serving](../ml_inference_service.md) | chat screenshot |
| MCP gateway + policy | Recorded | agentgateway `/mcp` — [agents](llm_platform_agents.md) | Within the scope of the linked doc |
| A2A gateway + policy | Recorded | routing by `x-kagent-host` — [agents](llm_platform_agents.md) | Within the scope of the linked doc |
| Coordinator multi-replica + scaling | Recorded | KEDA min 2 / max 3 on A2A requests at the gateway: 2→3→2 pods — [evidence](../../evidence/keda-coordinator-20261021.txt), [`coordinator-keda.yaml`](../../../infra/k8s/agents/coordinator-keda.yaml) | kagent UI chat screenshot |
| Publish coordinator to the registry | Recorded | [agents](llm_platform_agents.md) | Within the scope of the linked doc |
| Agent fleet / warm pool (substrate) | **On AWS** | Agent Substrate (kagent `controller.substrate`, gVisor WorkerPool, snapshot/rehydrate of idle agents) needs ate-system ~2–3 GB RAM — cannot run next to KFP/LLM on a 14 GB machine; locally replaced by controlled scale-to-0 (KEDA pause, scale qwen to 0) | AWS 6 Agents / 1 worker — [screenshot](../../evidence/aws/substrate-warmup-6-agents-1-worker-20261031.png) |
| Unit test coverage | Recorded | 85 tests, `serving/app.py` 98% (fixtures + MagicMock/AsyncMock + httpx MockTransport) — [evidence](../../evidence/serving-coverage-20261021.txt), [screenshot: 86 tests, `app.py` 98%, total 96%](../../evidence/serving-coverage-20261031.png), [`test_app.py`](../../../serving/tests/test_app.py) | Coverage as of the report snapshot |
| Equivalence partitioning / BVA | Recorded | boundary parametrize — `tests/unit/test_serve.py`, `test_agent_eval.py`, [web-api](../web_api_mcp.md) | Within the scope of the linked doc |
| Mutation testing | Recorded | mutmut — [web-api](../web_api_mcp.md) | Within the scope of the linked doc |
| Property-based (hypothesis) | Recorded | model: [`tests/unit/test_model_properties.py`](../../../tests/unit/test_model_properties.py) (idempotent prediction, retraining with the same seed gives the same model, the split leaks no future data; hypothesis found a single-class test case) + Web API: `test_idempotent` (Hypothesis) — repeating the same request and sending duplicate SKUs give the same result, [`serving/tests/test_app.py`](../../../serving/tests/test_app.py) | Within the scope of the linked doc |
| Locust load test (HTML) | Recorded | [web-api](../web_api_mcp.md) | Within the scope of the linked doc |
| CI/CD RAG pipeline | **On AWS** | `ci/jenkins/rag-pipeline.Jenkinsfile`: test → build → **push registry** → deploy → run DAG. Images go through the **local registry** `localhost:5001` (Jenkins pushes → k8s pulls; verified that the Feature API pod pulls `localhost:5001/platform-feature-api:<sha>`) — [cicd](../cicd.md). | AWS Jenkins EC2 green — [screenshot](../../evidence/aws/jenkins-rag-pipeline-20261031.png) |
| CI/CD MCP + agent (+ Langfuse prompt) | **On AWS** | `feature-api-agents.Jenkinsfile`: test → build → **push registry** → helm `--atomic` → **sync prompts from Langfuse** → deploy Agents → smoke 4 tools → **Eval agent** (below the 0.95 threshold ⇒ build red). Langfuse v2 holds 3 prompts (label production); Agents read them through the `systemMessageFrom` ConfigMap; changing a prompt to v2 in Langfuse ⇒ Agents pick it up after the sync (verified) — [agents](llm_platform_agents.md#system-prompts-from-langfuse) | AWS Jenkins EC2 green, eval 12/12 — [screenshot](../../evidence/aws/jenkins-feature-api-agents-20261031.png) |
| CI/CD coordinator | Partial | `coordinator.Jenkinsfile`: validate → sync Langfuse prompts → deploy → A2A smoke through the gateway → **Eval agent** | AWS deploy green, eval timeout (CPU LLM) — [CI/CD](../cicd.md) |
| CI/CD stream feature jobs | **On AWS** | `stream-features.Jenkinsfile`: test → build → **push registry** → deploy feast-pusher by SHA — [stream-feature](../stream_features.md) | AWS Jenkins EC2 green — [screenshot](../../evidence/aws/jenkins-stream-features-20261031.png) |
| Metric UI behind the gateway | Recorded | `metrics.` + `grafana.` — [gateway](../routing_gateway.md) | Within the scope of the linked doc |
| Log UI behind the gateway | Recorded | Loki through Grafana Explore — [observability](../observability.md) | Within the scope of the linked doc |
| Trace UI behind the gateway | Recorded | `traces.` (Jaeger) — [gateway](../routing_gateway.md) | Within the scope of the linked doc |
| Agent test UI | Recorded | `agent.` (kagent UI) | Within the scope of the linked doc |
| Agent registry UI | Recorded | `registry.` | Within the scope of the linked doc |
| Basic auth + rate limit | Recorded | 401/200/429 — [gateway](../routing_gateway.md) | Within the scope of the linked doc |
| Domain + HTTPS Web API | **On AWS** | sslip.io + internal CA; Let's Encrypt DNS-01 via DuckDNS is prepared but off by default (`scripts/setup/setup_letsencrypt.sh`) — [gateway](../routing_gateway.md) | AWS: `api.*.sslip.io`, Let's Encrypt cert (HTTP-01) — [gateway](../routing_gateway.md) |
| Terraform cloud services | **On AWS** | [Terraform AWS](../../../infra/aws/terraform/README.md): EC2/VPC/S3/IAM; [IaC](../iac.md) | AWS: EC2/VPC/S3/IAM + EKS, `terraform plan` screenshot — [IaC](../iac.md) |
| Ansible VM deploy | **On AWS** | [Ansible AWS](../../../infra/aws/ansible/README.md), [IaC](../iac.md) (Data Platform part) | AWS: `PLAY RECAP` screenshot on EC2 — [IaC](../iac.md) |
| Web API metrics | Recorded | feature-api `/metrics`; KServe `return_risk_*` — [serving](../ml_inference_service.md) | Within the scope of the linked doc |
| CPU/RAM/disk/network | Recorded | Prometheus + Grafana — [observability](../observability.md) | Within the scope of the linked doc |
| Logs | Recorded | Loki + Fluent Bit — [observability](../observability.md) | Within the scope of the linked doc |
| Traces | Recorded | agentgateway/kagent OTLP → Jaeger | Within the scope of the linked doc |
| LLM telemetry (tokens, TTFT, PII) | Recorded | tokens in/out/total per request, per-turn duration, **TTFT** (`gen_ai_server_time_to_first_token`), **PII**: the agentgateway guardrail blocks email/card/SSN (403), `guardrail_checks_total{action}` — dashboard "Data & AI Platform — Agents, LLM, MCP", [observability](../observability.md) | AWS screenshots: [TTFT/PII](../../evidence/aws/grafana-llm-ttft-pii-20261030.png), [tokens](../../evidence/aws/grafana-tokens-per-request-20261031.png) |
| Agent telemetry (agent/tool calls, errors) | Recorded | calls per Agent (`agentgateway_requests_total{route=a2a-*}`), per tool (`agentgateway_mcp_requests_total`), **errors per tool** (`mcp_tool_calls_total{tool,outcome}` in the Feature API) — dashboard "Data & AI Platform — Agents, LLM, MCP" | AWS screenshots: [Agent/MCP](../../evidence/aws/grafana-agents-mcp-gateway-20261030.png), [tool outcome](../../evidence/aws/grafana-mcp-tool-outcome-20261031.png) |
| A/B agents with different model configs | Recorded | data-agent vs data-agent-b — [ab-test](../ab_testing.md) | Within the scope of the linked doc |
| A/B of two LLM variants | Recorded | `platform-chat` 1:1 — [ab-test](../ab_testing.md) | Within the scope of the linked doc |
| Vault | Recorded | [vault](../security.md) | Within the scope of the linked doc |
| Agent eval harness | Recorded | A 100% vs B 91.7%; the gate runs in Jenkins (stage `Eval agent`, `scripts/run/eval_agent_ci.sh`) — [agent-eval](../agent_evaluation.md) | Stage View screenshot with the Eval stage |
| Drift monitoring | Recorded | Evidently, Sale Event alert — [drift](../drift_monitoring.md) | Within the scope of the linked doc |

Rows marked "Recorded" point to a report or receipt at the time of the run; they do not confirm the service is running. The component docs contain reproduction commands and note missing screenshots/logs; do not use the status in this table in place of a fresh run.

## Web API unit test coverage

`pytest serving/tests --cov=serving`: 86 tests pass, `serving/app.py` 98% (4 uncovered lines: `load_store` loading the real Feast,
and the HTTP error branch of the `get_policy_chunks` tool), total 96% > 90%. `locustfile.py` is 0% because it is a load-test script, not application
code. Tests use fixtures + `MagicMock`/`AsyncMock` in place of Feast, and `httpx.MockTransport` in place of KServe:
[`serving/tests/test_app.py`](../../../serving/tests/test_app.py).

![pytest coverage serving: 86 passed, app.py 98%, TOTAL 96%](../../evidence/serving-coverage-20261031.png)

## Reproducing and adding evidence

- Rerun CI/CD from the revision to verify, and save the Stage View, image digest and smoke/eval results.
- Sandbox and the agent fleet warm pool have EKS screenshots: [SandboxAgent calling a tool](../../evidence/aws/substrate-sandbox-agent-ui-20261031.png), [6 Agents / 1 worker](../../evidence/aws/substrate-warmup-6-agents-1-worker-20261031.png). The LLM is Qwen 1.5B on CPU; do not read this as GPU proof.
- [AWS EC2 + S3](../../shared/aws_deploy.md) serves the Data Platform; ML/LLM runs on EKS `platform-apps`. For the current AWS status see the [validation status](../../shared/aws_validation.md); the local table above keeps its historical scope.

[Docs index](../../README.md)
