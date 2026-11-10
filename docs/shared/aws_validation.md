# AWS validation status

Principle: every capability must have evidence of **deployment on AWS**; anything missing is open work.
Cloud evidence is recorded in [data-platform-evidence.md](../data-platform/evidence.md) under "On AWS" and in the AWS/EKS sections of the ML/LLM docs;
[ML/LLM Platform evidence](../ml-llm-platform/reference/ml_llm_platform_evidence.md) keeps the historical local table and points to the newer AWS results.

Legend: ✅ validated on AWS · ⚠️ runs on AWS but evidence is partial · ❌ local only / not yet.
For ⚠️ rows about screenshots: the capability runs on AWS; the detailed screenshot is local or has no matching AWS capture yet.
A green job status is not treated as a substitute for a detailed screenshot.

## Data Platform

| Capability | Status | Evidence / remaining work |
|---|---|---|
| Docker & Compose + Dockerfile optimization | ✅ | Compose runs on EC2; re-measured naive 2.4 GB → 698 MB, same 27 packages ([EC2 screenshot](../evidence/aws/docker-naive-vs-opt-ec2-20261031.png)) |
| Offline generator: skew, schema evolution, duplicates, source storage | ✅ | 100 GB generated on EC2, raw on S3 + Postgres on EC2 |
| Online generator: late arrival, stream duplicates | ✅ | Kafka + producer on EC2 |
| Spark baseline / skew / schema evolution / other + Spark UI | ✅ | Runs on AWS; skew has an AWS Spark UI ([Spark](../data-platform/processing_jobs.md#on-aws-2026-10-29)). The AWS Spark UI for Silver Amazon, opened from the event log, shows the two branches of the old/new schema → `Union` → `Window` dedup, dropping 3,098,355 duplicates (1.974%) ([screenshot](../data-platform/processing_jobs.md#on-aws-2026-10-28)). |
| Spark jobs integrated into the pipeline | ✅ | dp1–dp3 on Airflow on EC2 |
| Flink baseline / late arrival / other / window | ✅ | four Flink jobs run continuously in the **RUNNING** state (Flink UI screenshot, 6 slots); output: baseline 399,410 rows, dedup 92,157, late arrival 6,674 rows, all with `event_time <= watermark` ([Flink](../data-platform/processing_jobs.md#flink-on-aws-2026-10-28)). The earlier (bounded, FINISHED) run is kept as reference figures. |
| Lakehouse optimization (compaction/partitioning) | ✅ | shopee daily→monthly, 180→6 files, 30-day filter 8×; Trino screenshot counting files in the snapshots before and after (180 → 6, same 4,689,957 rows) ([storage.md](../data-platform/data_storage.md)) |
| Warehouse indexing | ✅ | `dp4_warehouse` 37M rows, EXPLAIN 42.6 s → 1.5 s ([storage.md](../data-platform/data_storage.md)) |
| DP1 / DP2 / DP3 ingest + validate | ✅ | Airflow on AWS; screenshots of Admin → Connections and Variables on Airflow on AWS ([Orchestration](../data-platform/pipeline_orchestration.md)) |
| DataHub lineage + validation DP1/DP2/DP3 | ✅ | DataHub on EC2, 6 DAGs: 22 datasets, 30 jobs, 16 assertions |
| Schema: zones, SCD2, feat_, dim–fact, naming | ✅ | Runs on AWS; DBeaver ER Diagram screenshot of the Gold schema with connecting lines (virtual foreign keys) on Trino on AWS, plus dbdiagram and DBML. Screenshots of the zone/table list and DESCRIBE of SCD2/feature tables in [Schema](../data-platform/schema_design.md#on-aws). |
| Trino federation | ✅ | Trino on EC2, Iceberg S3 = Postgres 4,517 rows ([trino.md](../data-platform/trino_federation_and_cdc.md)) |
| WAL CDC to Kafka (Debezium) | ✅ | Debezium on EC2, c/u/d probe |

## ML/LLM Platform

Evidence: [LLM and agents](../ml-llm-platform/agents.md),
[RAG](../ml-llm-platform/rag_pipeline.md#on-aws), [Web API](../ml-llm-platform/web_api_mcp.md#on-eks),
[Observability](../ml-llm-platform/observability.md#on-eks), [CI/CD](../ml-llm-platform/cicd.md#on-aws),
[eval](../ml-llm-platform/agent_evaluation.md#on-aws), [drift](../ml-llm-platform/drift_monitoring.md#on-aws-pipeline-a-2026-10-31);
screenshots and receipts in `evidence/aws/`.

| Capability | Status | Evidence / remaining work |
|---|---|---|
| LLM inference platform + benchmark | ✅ | llama.cpp Qwen 2.5 1.5B on EKS; bench with 6/3/4 threads (`bench-llm-eks-t*.json`), 3 chosen. Deployment screenshots (pods, PVC, logs) and **llm-d-benchmark** in run-only mode against the llama.cpp endpoint, measuring both **baseline and optimized** on the same cluster (Nov 10): each configuration at 3 load levels, 42/42 requests, 0 errors; at 0.4 req/s optimized cuts TTFT p50 by 46% and p90 by 40%; at low load baseline is faster (4 threads vs 3) ([LLM](../ml-llm-platform/llm_inference_platform.md#baseline-vs-optimized-nov-10-2026)) |
| Custom LLM model | ✅ | Self-hosted GGUF on EKS |
| agentgateway + security/policy | ✅ | `gateway-smoke-20261109.json` **24/24** after splitting out the MCP pod, with an allow/deny table per policy (API key 401, PII 403, rate limit 429, MCP tools outside the allowlist blocked, A2A per Agent) and a ConfigMap configuration screenshot ([Gateways](../ml-llm-platform/agent_gateways.md)) |
| Global model config | ✅ | `default-model-config` → agentgateway; `kubectl` screenshot of the ModelConfig and each Agent's model ([Model Config](../ml-llm-platform/global_model_config.md)) |
| Agent registry | ✅ | agentregistry 0.4.0, 1 MCP + 3 agents (`agent-registry-ui`) |
| RAG pipeline (Airflow) + governance (DataHub) | ✅ | `ml_rag_ingest` 6/6 tasks green on Airflow on EC2, including `publish` ([screenshot](../evidence/aws/airflow-ml-rag-ingest-publish-20261031.png)); DataHub on EC2 lineage corpus → S3 raw → chunks → embedding → pgvector → validate ([screenshot](../evidence/aws/datahub-rag-lineage-20261031.png)). Fixed: ASCII escaping of value aspects, `DataJobInfo.type` as a union |
| Materialize offline → online | ✅ | `ml_feature_materialize` 4/4 |
| Push streaming features → offline + online | ⚠️ | 2 PyFlink jobs `ml-sku-return-24h-online` / `-offline` on AWS (Flink 2.2, [stream](../ml-llm-platform/stream_features.md)): Flink UI, metrics, checkpoints; `/features` goes from `null` to a value; reconciliation 56,231/56,231 windows, 2,000/2,000 SKUs up to the closed watermark. **No final figure for the full replay yet.** Replayed data |
| KFP training ≥4 steps | ✅ | `train-aws-3` 5/5 |
| MLflow model + data versioning + tags | ✅ | v1 `production` runs on AWS; screenshots of the `data.*` Tags and the `data/train`, `data/test` Artifacts of run `mysterious-flea-253`, a script checking artifact SHA-256 against the tags (match) and tracing model v1 → run ([Training](../ml-llm-platform/training_pipeline.md#on-aws)). The MLflow run ↔ KFP run link is based on timestamps. |
| KServe model-inside-image | ✅ | predict OK; 2→3→2 with KEDA ([Inference](../ml-llm-platform/ml_inference_service.md)) |
| FastAPI + pydantic + healthcheck / async / MCP helm rolling + fallback | ✅ | Feature API with at least 2 pods on AWS, Swagger; MCP server in its own pod `feature-mcp`; both autoscale 2→3→2; Liveness/Readiness/Startup probes, `/healthz` and `/readyz` 200 in each pod and via Swagger; Helm rolling update, then a broken release rolls back automatically (revision 13 → 14 `failed` → 15 `Rollback to 13`), 763/763 requests 200 ([Web API](../ml-llm-platform/web_api_mcp.md)) |
| Agent 1 multi-replica + KEDA, Sandbox, publish | ✅ | data-agent 2 pods + KEDA 2→3→2 ✅, published to registry ✅; Sandbox Agent Substrate 0.0.9 (gVisor) on EKS, `data-agent-sandbox` calls the `get-sku-features` tool through the gateway and returns numbers ([screenshot](../evidence/aws/substrate-sandbox-agent-ui-20261031.png)); the 1.5B model copies `labeled_count` wrong (83,084 instead of 830847), positive/rate correct |
| Agent 2: online features + predict + RAG | ⚠️ | Full set "equivalent" to Agent 1 ([checklist](../ml-llm-platform/agents.md#model-agent-has-the-same-full-set-as-data-agent)): tools/chat on AWS; KEDA `model-agent` 2 → 3 → 2; gVisor Sandbox; published to registry; MCP through the gateway smoke 24/24 after splitting out the `feature-mcp` pod; KServe `return-risk` and `feature-mcp` 2 → 3 → 2. **Not passing:** eval predict 12/12 but policy 3/5, below the 0.95 threshold; Jenkins #13 is red because Qwen 1.5B sometimes does not call the tool ([Eval](../ml-llm-platform/agent_evaluation.md)). |
| MCP gateway / A2A gateway | ✅ | `/mcp` route, A2A by header; Grafana counts per route |
| Coordinator multi-replica + KEDA, publish | ✅ | Runs on AWS, published to registry; scales 2 → 3 → 2 on EKS (logs, HPA/pod screenshots), [Coordinator](../ml-llm-platform/coordinator.md) |
| Warm Up (Agent Substrate) | ✅ | 6 SandboxAgents Ready on 1 WorkerPool `kagent-default` (DESIRED/REPLICAS 1), golden snapshot ([screenshot](../evidence/aws/substrate-warmup-6-agents-1-worker-20261031.png)) |
| Unit coverage, EP/BVA, mutation, property-based | ✅ | Checked at code level (PR testing), independent of where it runs; real terminal screenshots of coverage, EP/BVA, hypothesis ([Validation](../ml-llm-platform/validation_verification.md)) |
| Locust load test of the Web API | ✅ | Locust → Feature API on EKS: 3,004 requests, 0 errors; re-run 3,065 requests, 0 errors, p50 250 ms, with a report screenshot ([Validation](../ml-llm-platform/validation_verification.md)) |
| CI/CD RAG / agent / coordinator / stream features | ⚠️ | Jenkins on EC2 → ECR/EKS/Compose: `feature-api-agents` #13 red at RAG eval 3/5 (#8 green, agent eval 12/12), `rag-pipeline` #2, `stream-features` #7 green (2 PyFlink jobs, [screenshot](../evidence/aws/jenkins-stream-features-7-20261109.png)), Flink SQL version #2 ([screenshot](../evidence/aws/jenkins-feature-api-agents-20261031.png), [rag](../evidence/aws/jenkins-rag-pipeline-20261031.png), [stream](../evidence/aws/jenkins-stream-features-20261031.png)); `coordinator`: `Validate`/`Sync`/`Deploy` green, and the `Eval agent` gate passes 12/12 at #9 but only scores routing (the first two runs were red or did not finish because the CPU LLM is slow under overlapping load; see [Agent Evaluation](../ml-llm-platform/agent_evaluation.md#coordinator-on-aws-2026-11-04)) |
| NGINX: Grafana, logs, traces, agent UI, registry UI | ✅ | HTTPS routes + LE certificate: grafana., traces., agent., registry. |
| Basic auth + rate limit | ✅ | 401 without password; rate limit in the gateway smoke |
| Domain + HTTPS for the Web API | ✅ | `api.15-134-182-157.sslip.io` with LE certificate, Swagger over HTTPS |
| Terraform / Ansible | ✅ | Terraform AWS + Ansible actually run |
| Observability: Web API metrics, Prom+Grafana, logs, traces | ✅ | Feature API/KServe panels, Loki `{namespace="feature-store"}`, Jaeger traces for agentgateway; success/failure panels per Agent ([Observability](../ml-llm-platform/observability.md)) |
| LLM telemetry (tokens, TTFT…) / Agent telemetry | ✅ | Grafana tokens, TTFT, time/token, Agent calls + MCP tool calls |
| A/B agent / A/B LLM | ✅ | A/B dashboard with all six panels populated (traffic split, req/s, p95, tokens/s, tokens/request, A/B agent route) ([A/B](../ml-llm-platform/ab_testing.md)). EKS: LLM `platform-chat` split 1:1 between Qwen 1.5B/0.5B, Grafana 9.13 vs 8.07 req, p95 ~10 s vs ~80 s ([screenshot](../evidence/aws/grafana-ab-llm-20261031.png), [Prometheus](../evidence/aws/ab-llm-prometheus-20261031.json)); agent on the same 12 SKUs: A (1.5B) 12/12 ~10 s/question, B (0.5B) 4/12 ~5 s/question ([A](../evidence/aws/agent-eval-data-agent-20261031.jsonl), [B](../evidence/aws/agent-eval-data-agent-b-20261031.jsonl)); split route `/a2a/data-agent-ab/` 6 questions 2/6 ([jsonl](../evidence/aws/agent-eval-data-agent-ab-20261031.jsonl)) |
| Vault | ✅ | Vault on EC2 (EBS), KV v2, ingress password |
| README and documentation | ✅ | The README has the AWS Data/ML and LLM/Agent diagrams, a table of contents, the repo layout and documentation links; detailed screenshot limitations are recorded separately in the ⚠️ rows. |
| Agent eval harness / Drift monitoring | ✅ | Agent eval harness: Jenkins `feature-api-agents` #8 passes 12/12 ≥ 0.95 ([JSONL](../evidence/aws/agent-eval-data-agent-20261031.jsonl), [screenshot](../evidence/aws/jenkins-feature-api-agents-20261031.png)); SKUs with counts below 7 digits were chosen because the PII mask still hides large counts. Drift monitoring: EKS Job `mle-drift`, Evidently 0.7.23 + pandas 2.3.3: stable raises no alert, channel_shift flags `seller_sku` JS 0.2028 > 0.2 ([JSON](../evidence/aws/mle-drift-20261031.json)); re-run with identical results, plus an Evidently report screenshot from the EKS pod ([Drift](../ml-llm-platform/drift_monitoring.md)). |

## Assessment

- Self-assessment based on the current AWS evidence. The ⚠️ rows from the previous review have since received AWS screenshots or logs (Spark UI Union/Window dedup, Flink running continuously in RUNNING and late arrival,
  DBeaver ERD, compaction before/after, MLflow hashes/artifacts, Helm rolling/rollback, KEDA for Model Agent and Coordinator, A/B with all panels).
- **Still open: the Model Agent's RAG eval.** `feature-api-agents` #13 is red: policy 3/5, below the 0.95 threshold, because Qwen 1.5B sometimes writes the tool call as text. The Coordinator passes 12/12 (#9), but the gate only scores routing.
- Weaknesses to be aware of despite the evidence: the `mcpAuthorization` case can only record "tool outside the allowlist rejected" (the `Unknown tool` response does not distinguish gateway from backend); the MLflow ↔ KFP link is based on timestamps;
  stream features use replayed data, with no real Return stream yet; A/B figures were read by eye from graphs, small sample (40 requests); llm-d-benchmark ran only one short run per configuration, with no confidence intervals, and baseline differs from optimized in several parameters at once; the Feature Store's A/B batch publication (DB1/DB2) has not run on AWS, and the reload used the old incremental flow.
- EKS context `platform-apps`: **2 r6i.xlarge nodes** (each 4 vCPU/32 GB). Together with the EC2 Data Platform `r6i.xlarge`: 12/16 vCPU quota in total.

## AWS configuration

EC2 Data Platform `r6i.xlarge` (4 vCPU/32 GB) + EKS `platform-apps` with 2 `r6i.xlarge` nodes: 12/16 vCPU quota.
~1.1 USD/hour with both running, ~0.18 USD/hour with everything stopped (EKS control plane + EBS); run in sessions, shut down when idle.
The sandbox uses Agent Substrate 0.0.9 (matching kagent 0.10.3), which sets up the gVisor runtime itself through its node agent
([kagent example](https://www.kagent.dev/docs/kagent/examples/agent-substrate)); code in `scripts/setup/deploy_substrate.sh` + `infra/k8s/agents/sandbox/`.
