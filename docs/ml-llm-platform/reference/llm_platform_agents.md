<!-- Purpose: local Agent platform (LLM, agentgateway, kagent, Data Agent) — -->
# Agent platform (local, kind `platform`)

```
kagent UI ─► Data Agent (kagent) ──/v1 (API key)──► agentgateway ──► llama.cpp Qwen2.5-1.5B (ns llm)
                                └──/mcp (API key)──►      │       ──► feature-mcp /mcp/ (ns feature-store)
                                                           └ /metrics :15020
```

Deploy everything: `bash scripts/setup/deploy_kind_agents.sh` (requires the cluster and the Feature API to be running:
[`docs/ml-llm-platform/web_api_mcp.md`](../web_api_mcp.md)).

| Part | File | Namespace |
|---|---|---|
| LLM server | [`infra/k8s/agents/llm.yaml`](../../../infra/k8s/agents/llm.yaml) | `llm` |
| agentgateway | [`infra/k8s/agents/agentgateway.yaml`](../../../infra/k8s/agents/agentgateway.yaml) | `agentgateway` |
| kagent (Helm values) | [`infra/k8s/agents/kagent-values.yaml`](../../../infra/k8s/agents/kagent-values.yaml) | `kagent` |
| Data Agent + RemoteMCPServer | [`infra/k8s/agents/data-agent.yaml`](../../../infra/k8s/agents/data-agent.yaml) | `kagent` |

## LLM server (local replacement for llm-d)

The llm-d configuration needs a GPU. Locally we use the **llama.cpp server** (CPU), with an OpenAI-style API,
model **Qwen2.5-1.5B-Instruct Q4_K_M** (file ~1 GB, RAM ~1.3 GB). Why this model: it is the smallest that still calls
tools reliably. llm-d is reserved for when a GPU is available.

- `--jinja` enables the chat template with tool calling; `--ctx-size 8192`, `--parallel 2` (4096 tokens per slot).
- The model is downloaded once into the `models` PVC; the image is pinned by digest (build b11749).
- `--metrics` exposes `/metrics` (tokens/s, request count) for Prometheus later.
- When done, `kubectl -n llm scale deploy/qwen --replicas=0` to free RAM.

Checked with the question "What is the return rate of SKU sku-00042?" plus tool definitions →
`finish_reason: tool_calls`, `get_sku_features({"seller_skus": ["sku-00042"]})`.

### Benchmark and tuning

Script [`scripts/evidence/bench_llm.py`](../../../scripts/evidence/bench_llm.py): Agent-style chat requests (system prompt + 3 tools,
~520 prompt tokens, at most 64 answer tokens), 8 requests per concurrency level, measured by calling `qwen.llm:8080` directly.
CPU i5-11400H 6 cores / 12 threads.

| Configuration | Concurrency | p50 (s) | p95 (s) | requests/s | output tokens/s |
|---|---:|---:|---:|---:|---:|
| A: threads 4, parallel 2, ctx 8192, KV f16 | 1 | 2.68 | 2.79 | 0.413 | 21.4 |
| | 2 | 4.71 | 7.87 | 0.381 | 20.1 |
| | 4 | 6.53 | 9.56 | 0.475 | 25.3 |
| **B: threads 6, parallel 4, ctx 16384, flash-attn, KV q8_0** | 1 | 2.62 | 2.92 | 0.413 | 21.2 |
| | 2 | 4.57 | 8.85 | 0.372 | 19.3 |
| | 4 | **6.29** | **7.00** | **0.578** | **29.6** |

- 4 concurrent requests (Coordinator + 2 Agents + user): B raises requests/s by 22% and tokens/s by 17%, and cuts p95 by 27%, thanks to 4 slots
  processing in parallel instead of 2 slots + a queue.
- 1–2 requests: the same — decoding is CPU-bound (RAM bandwidth), so more threads do not help.
- The q8_0 KV cache keeps RAM at 1.34 GB despite double the ctx (each slot still has 4096 tokens, enough for the Agent prompt + tools).
- B was chosen ([`llm.yaml`](../../../infra/k8s/agents/llm.yaml)). Data: [`evidence/llm-benchmark-20261021.jsonl`](../../evidence/llm-benchmark-20261021.jsonl).
- With a GPU: llm-d; rerun the same script to compare.

### Re-measurement on EKS

The `r6i.xlarge` node has only 4 vCPUs, but configuration B sets 6 threads (based on the 6-core laptop). Same script, called through
`kubectl port-forward svc/qwen`, changing only `--threads`:

| Threads | Concurrency | p50 (s) | p95 (s) | requests/s | output tokens/s |
|---|---:|---:|---:|---:|---:|
| 6 (old) | 1 | 5.66 | 6.25 | 0.188 | 9.9 |
| | 2 | 8.63 | 14.70 | 0.204 | 10.6 |
| | 4 | 11.02 | 12.60 | 0.342 | 17.7 |
| **3** | 1 | 5.38 | 5.62 | 0.204 | 10.7 |
| | 2 | 7.70 | 14.97 | 0.214 | 11.3 |
| | 4 | **10.96** | **12.44** | **0.345** | **17.8** |
| 4 | 1 | **4.74** | 5.19 | 0.229 | 12.0 |
| | 2 | 7.60 | 13.93 | 0.225 | 11.8 |
| | 4 | 12.57 | 19.14 | 0.269 | 14.0 |

- 6 threads > 4 vCPUs: the threads compete for CPU, losing or tying at every level.
- 4 threads is fastest with 1 request, but at 4 requests p95 rises to 19.1 s and tokens/s drops 21% compared with 3 threads:
  llama.cpp takes all 4 vCPUs, competing with the Agents, KEDA and exporters on the same node.
- **3 threads** chosen: the Coordinator calls 2 Agents in parallel so the real load is 2–4 requests; 1 vCPU is left for other pods.
- 8 requests per level ⇒ differences under ~10% are treated as a tie. Data: [`evidence/aws/bench-llm-eks-t6-20261030.json`](../../evidence/aws/bench-llm-eks-t6-20261030.json),
  [`t3`](../../evidence/aws/bench-llm-eks-t3-20261030.json), [`t4`](../../evidence/aws/bench-llm-eks-t4-20261030.json).

## agentgateway: gateway for LLM and MCP

agentgateway v1.6.0 (standalone, file-configured), a single port `:3000`:

| Path | To | Policy |
|---|---|---|
| `/v1/chat/completions` | llama.cpp (`provider: custom`, format `completions`) | API key, rate limit burst of 60 requests + refill 1/s |
| `/mcp` (route) | `feature-mcp` `/mcp/` | API key, `mcpAuthorization` allows only the 4 approved tools |
| header `x-kagent-host: <agent>.kagent` | the Agent's Service (A2A JSON-RPC) | API key, `a2a` policy; the path kagent uses when `proxy.url` is enabled |
| `/a2a/<agent>/` | same as above, for clients outside the cluster | API key, `a2a`, strips the path prefix |

- The API key is in the Secret `agentgateway-keys`, generated once by the script; the kagent side uses the same key through the Secret
  `kagent-agw-key` (`AGW_AGENT_KEY` for the model, `AUTH_HEADER` for MCP).
- Agents do not know the real address of the LLM or the Feature API; changing the model/backend only touches the gateway.

Checks (called from a pod in the cluster):

| Test | Result |
|---|---|
| LLM without key | 401 |
| LLM with key | 200, normal answer |
| MCP with no / wrong key | 401 |
| MCP with key: `initialize` → `tools/list` → `tools/call get_sku_features` | 200, exactly 1 tool, returns features |
| 80 concurrent LLM requests (50 threads) | **62 × 200, 18 × 429** in 4.9 s: rate limit in effect |
| 300 concurrent MCP requests | 300 × 200: rate limit **not** in effect for MCP (version 1.6.0, tried both the `mcp:` block and a route) |
| Agent card via `/a2a/data-agent/.well-known/agent-card.json` | 200, `url` = `http://agentgateway:3000/a2a/data-agent` |
| A2A without key | 401 |

A2A points directly at the Agent's Service, not through the kagent controller's A2A proxy: the card returned by that proxy
lacks the `url` field, so the `a2a` policy reports `agent card missing URL` (503).

**Agent-to-Agent calls through the gateway.** kagent's Helm value `proxy.url` = agentgateway. kagent then changes the
Agent-as-tool URL `http://data-agent.kagent:8080` to `http://agentgateway…:3000` and adds the header
`x-kagent-host: data-agent.kagent` (function `applyProxyURL` in `adk_api_translator.go`). The gateway has one route
per Agent matching that header. The Coordinator sends `Authorization` from a Secret (`headersFrom`), so A2A is also
blocked without a key.

MCP is set up as a `/mcp` route instead of the shorthand `mcp:` block: the shorthand block accepts **every** path and sits in front of
the other routes, so header-based A2A requests get caught by it (error `mcp: client must accept text/event-stream`).

Checking the card via header and via `/a2a/data-agent/` both return 200; MCP without key 401, with key lists the tools;
LLM `/v1/models` 200.

Remaining limitations: MCP has no rate limit at the gateway yet; the rate limit is an in-memory counter (not shared between replicas).

## kagent and the Data Agent

kagent 0.10.3 is installed with Helm using [`kagent-values.yaml`](../../../infra/k8s/agents/kagent-values.yaml): all sample agents and
tools (k8s, istio, cilium, grafana-mcp…) are disabled, keeping only the controller, UI, the bundled Postgres and kmcp (~600 MB RAM).

**Shared model config**: Helm generates `ModelConfig default-model-config`, provider `OpenAI`,
model `qwen2.5-1.5b-instruct`, `baseUrl` = agentgateway `/v1`, API key from the Secret `kagent-agw-key`.
Every Agent references this same name; switching the LLM (e.g. to llm-d on GPU) only changes the gateway, not the Agents.

**Data Agent** ([`data-agent.yaml`](../../../infra/k8s/agents/data-agent.yaml)): a Declarative Agent,
with tools from `RemoteMCPServer feature-tools` pointing at the gateway `/mcp` with an `Authorization` header read from a Secret.
Short system prompt that forces tool calls instead of guessing numbers (the 1.5B model hallucinates easily).

Checked: Agent `READY=True ACCEPTED=True`, pod Running; `RemoteMCPServer` reports
`discoveredTools: get_sku_features` (kagent calls `tools/list` through the gateway with the API key itself).
Not chat-tested yet: the LLM is scaled to 0 to free RAM for Kubeflow.

Replica count: no `replicas` declared in the Agent ⇒ kagent does not override the Deployment's `spec.replicas`
(`manifest_builder.go`: "only set replicas if explicitly specified, so not to override HPA settings"),
so KEDA controls it in the autoscaling step.

## Agent 2 (Model Agent) and Coordinator

- [`model-agent.yaml`](../../../infra/k8s/agents/model-agent.yaml): tools `get_sku_features` (Online Feature Store),
  `predict_return_risk` (KServe `return-risk`, protocol v1; the model reads features from Feast itself so the tool only sends
  order information), `search_policy_chunks` (RAG). The score is a ranking signal, not a probability.
- [`coordinator.yaml`](../../../infra/k8s/agents/coordinator.yaml): two Agent-type tools (data-agent, model-agent),
  called through the gateway.
- KEDA: [`data-agent-keda.yaml`](../../../infra/k8s/agents/data-agent-keda.yaml) (min 2, max 4, by A2A request rate
  at the gateway, read from Prometheus [`prometheus-values.yaml`](../../../infra/k8s/observability/prometheus-values.yaml)).

### Chat test (A2A `message/send` through the gateway `/a2a/<agent>/`)

| Question | Agent | Tool called | Result |
|---|---|---|---|
| "What are the return features of SKU sku-00002?" | data-agent | `get_sku_features(["sku-00002"])` | correct Feast numbers (labeled 0, rate null), 9.7 s |
| "A buyer on tiktok_shop just ordered 3 units of sku-00002 for 30000 cents. What is the return risk?" | model-agent | `predict_return_risk(...)` with all 4 parameters correct | score 0.346, abstain (matches KServe `return-risk` v3), 14 s |
| "Can a TikTok Shop buyer get a refund without sending the item back?" | model-agent | `search_policy_chunks` | correct answer per the "Refund only" chunk |
| "What is the return history of sku-00099?" | coordinator | `kagent__NS__data_agent` | routed correctly, answered from data-agent |
| "Return risk of an order of 2 units of sku-00003 on amazon for 4500 cents?" | coordinator | `kagent__NS__model_agent` | score 0.3047 |

The gateway records A2A coordinator → child agent via the header-based route (`default/a2a-model-agent`,
`default/a2a-data-agent` in `agentgateway_requests_total`): Agents do not call each other directly.

**Limits of the 1.5B model (CPU).** A policy question goes through 2 Agents (coordinator → model-agent → RAG):
- Initially the coordinator answered by itself without calling an Agent ("Walmart 90 days" — wrong, the document says 30). Fixed by:
  Agent descriptions in English with keywords (the LLM sees Agent descriptions as tool descriptions) + the system prompt "router,
  must call exactly 1 tool". After the fix: 3/3 test questions were routed to the right Agent.
- model-agent does call `search_policy_chunks` but summarizes wrongly (taking "7–14 days" from the store drop-off section instead of
  "30 days") or makes up playbook content. Asking model-agent directly gives the right answer.
- Conclusion: the architecture (routing, tools, gateway) works; multi-step answer quality is limited by the 1.5B model.
  With a GPU, replace it with a larger model via llm-d, changing only the gateway/ModelConfig.

### Autoscaling with KEDA

Prometheus 29.36.2 (server only) + KEDA 2.20.2. `ScaledObject data-agent`: min 2, max 3 (local), trigger
Prometheus `sum(rate(agentgateway_requests_total{route="default/a2a-data-agent"}[1m]))`, threshold 0.5 requests/second
per pod. Load of 3 requests/second (fetching the agent card via the header route) for 150 s:

| Time | Load / pod | Running pods |
|---|---|---|
| 0–30 s | 0 → 0.40 | 2 |
| 45 s | 0.82 > 0.5 | **3** (HPA: "above target") |
| end of load + 5 minutes | 0 | **2** (HPA: "All metrics below target") |

Log: [`evidence/keda-data-agent-20261021.txt`](../../evidence/keda-data-agent-20261021.txt). The load uses agent-card requests
(cheap, no LLM call) to measure the scaling mechanism without congesting the CPU LLM.

After measuring: qwen, model-agent, coordinator, kagent-ui scaled to 0; ScaledObject data-agent paused at 0
(`autoscaling.keda.sh/paused-replicas: "0"`) to free RAM for Kubeflow. To re-enable: remove the annotation, scale the
deployments back to 1, `kubectl -n llm scale deploy/qwen --replicas=1`.

## Agent registry (governance)

agentregistry 0.4.0 ([`agentregistry-values.yaml`](../../../infra/k8s/agents/agentregistry-values.yaml),
`scripts/setup/deploy_kind_agentregistry.sh`), DB = database `agentregistry` on the pgvector Postgres (saves one Postgres
image). Published with `arctl apply` ([`scripts/setup/publish_agentregistry.sh`](../../../scripts/setup/publish_agentregistry.sh),
declared in [`infra/k8s/agents/registry/`](../../../infra/k8s/agents/registry/feature-tools.yaml)):

| Kind | Name | Notes |
|---|---|---|
| MCPServer | feature-tools | `remote: streamable-http` = agentgateway `/mcp` (4 tools) |
| Agent | data-agent, model-agent | image `golang-adk:0.10.3`, protocol A2A, `mcpServers: feature-tools` |
| Agent | coordinator | image `golang-adk:0.10.3`, protocol A2A |

Checked: `arctl get mcpservers` / `arctl get agents` list all 1 + 3. Errors encountered: MCPServer does not allow declaring
both `source` and `remote`; `protocol` must be uppercase `A2A`; `modelProvider/modelName` are deprecated (warning only) so they were
dropped and the model was written into the description. The registry has no login of its own ⇒ only exposed through an ingress with basic auth.

Agent/LLM monitoring: [`docs/ml-llm-platform/observability.md`](../observability.md).

## System prompts from Langfuse

Agents take their system prompts from a prompt management tool. Langfuse **v2.95.11**
([`langfuse.yaml`](../../../infra/k8s/agents/langfuse.yaml), ns `langfuse`) needs only Postgres ⇒ it uses the database `langfuse` on
pgvector; v3+ also needs ClickHouse/Redis/S3, too heavy for a local machine.

```
Langfuse prompt platform-<agent> (version, label production)
   │ scripts/setup/sync_agent_prompts.py  (Jenkins: stage "Sync prompts from Langfuse")
   ▼
ConfigMap kagent/agent-prompts (key = Agent name, annotation = Langfuse version)
   │ Agent.spec.declarative.systemMessageFrom
   ▼
kagent rebuilds the Agent config (Secret <agent>/config.json)
```

- Deploy + initial load: `bash scripts/setup/deploy_kind_langfuse.sh` (source prompts in
  [`infra/k8s/agents/prompts/`](../../../infra/k8s/agents/prompts/README.md), only created when Langfuse does not have them yet). The API key is in
  the Secret `kagent/langfuse-api`; the admin UI password is in the Secret `langfuse/langfuse-env`.
- Checked loading 3 prompts (version 1, production). Created `platform-coordinator` **version 2** in Langfuse (adding
  "Always answer in English.") → ran sync → the coordinator's `config.json` contains the new sentence after ~20 s, with no redeploy.
- Error encountered: 2 Langfuse pods ran the Prisma migration at the same time ⇒ the migration broke (P3009). Fix: `strategy: Recreate`,
  drop/recreate an empty database.
- Local: Langfuse is scaled to 0 when not in use.

## Coordinator autoscaling

[`coordinator-keda.yaml`](../../../infra/k8s/agents/coordinator-keda.yaml): min 2, max 3, trigger
`sum(rate(agentgateway_requests_total{route=~"default/a2a-coordinator(-path)?"}[1m]))`, threshold 0.5/pod.
Load of 3 requests/second for 150 s: **2 → 3 pods** at second 45 (0.87/pod), **3 → 2** 5 minutes after the load ended
([evidence](../../evidence/keda-coordinator-20261021.txt)). After measuring: ScaledObject paused at 0 to free RAM.

## On EKS

Three Agents run on EKS `platform-apps`, all using Qwen 2.5 1.5B, reached through `https://agent.<ip>.sslip.io`
(basic auth `seller`, password in Vault `secret/platform/agent-ui`).

![kagent UI: coordinator, data-agent, model-agent all on model qwen2.5-1.5b-instruct](../../evidence/aws/kagent-ui-agents-20261030.png)

**Chat through the Coordinator.** Question: "What is the return rate of SKU APP-00042 on amazon US, and what is the
predicted return risk for an order of 1 unit at 25 USD?". The Coordinator calls data-agent (tool `get_sku_features`) then
model-agent (tool `predict_return_risk`) via A2A on agentgateway, then combines the two numbers:

![data-agent returns APP-00042 features: 15306 labeled rows, 2599 Returns, rate 0.1698](../../evidence/aws/kagent-ui-coordinator-data-agent-20261030.png)

![model-agent calls predict-return-risk, risk score 0.1661](../../evidence/aws/kagent-ui-model-agent-tool-20261030.png)

![Coordinator's final answer: rate 0.1698 and risk score 0.1661](../../evidence/aws/kagent-ui-coordinator-answer-20261030.png)

Both numbers match `/features` (0.1698) and `/predict` (0.166) called directly on the Feature API.

**Bug found during the real run.** In the first chat, the answer showed `<PHONE_NUMBER>` instead of the Return rate:
the Feature API returned `0.16980269175486737` (17 digits), and the `phoneNumber` guardrail on agentgateway's response side
mistook it for a phone number and masked it.

![First chat: Coordinator returns <PHONE_NUMBER> instead of the Return rate](../../evidence/aws/kagent-ui-pii-mask-before-20261030.png)

Fixed at the source: [`serving/app.py`](../../../serving/app.py) rounds rates and scores to 4 digits (`Rounded`), test
`test_long_decimals_rounded_for_pii_guardrail`; image `813627b-pii1`, `helm upgrade` revision 2 (rolling).
data-agent's Activity shows the same question before and after the fix:

![data-agent Activity: before 0.16980269175486737, after 0.1698](../../evidence/aws/kagent-ui-rounding-before-after-20261030.png)

Known limitation: when the question does not state quantity/price, the 1.5B model fills in `ordered_product_net_minor` by itself (once
`1000000000000000`, exceeding the API limit ⇒ the tool fails and the Agent misinterprets it as "too little history"). Stating the price in the
question gives the right result.

**KEDA on EKS.** Same measurement as locally: 3 requests/second (agent card, no LLM call) to route `a2a-data-agent`
for 150 s ([log](../../evidence/aws/keda-data-agent-20261030.txt)):

| Time | Load / pod | Pods |
|---|---|---|
| 0–21 s | 0 | 2 |
| 42 s | 933m > 500m | **3** (HPA "above target") |
| 206 s | 45m (load ended) | 3 |
| 491 s | 0 | **2** (HPA "All metrics below target") |

**Agent registry.** agentregistry on EKS (`https://registry.<ip>.sslip.io`, basic auth `ops`): 3 Agents published
with descriptions, model and the `latest` label, plus 1 MCP server.

![agentregistry: coordinator, data-agent, model-agent](../../evidence/aws/agent-registry-ui-20261030.png)

**Prompts from Langfuse.** Three `platform-*` prompts with the `production` label, created via the API by the seed script and then loaded into the ConfigMap
`agent-prompts`:

![Langfuse: 3 prompts platform-coordinator, platform-model-agent, platform-data-agent](../../evidence/aws/langfuse-prompts-20261030.png)

Content of each prompt: [coordinator](../../evidence/aws/langfuse-prompt-coordinator-20261030.png),
[model-agent](../../evidence/aws/langfuse-prompt-model-agent-20261030.png),
[data-agent](../../evidence/aws/langfuse-prompt-data-agent-20261030.png).
