<!-- Mục đích: nền tảng Agent local (LLM, agentgateway, kagent, Data Agent) — -->
# Nền tảng Agent (local, kind `platform`)

```
kagent UI ─► Data Agent (kagent) ──/v1 (API key)──► agentgateway ──► llama.cpp Qwen2.5-1.5B (ns llm)
                                └──/mcp (API key)──►      │       ──► Feature API /mcp/ (ns feature-store)
                                                           └ /metrics :15020
```

Dựng tất cả: `bash scripts/setup/deploy_kind_agents.sh` (cần cụm và Feature API đã chạy:
[`docs/edai-2/web_api_mcp.md`](../web_api_mcp.md)).

| Phần | File | Namespace |
|---|---|---|
| LLM server | [`infra/k8s/agents/llm.yaml`](../../../infra/k8s/agents/llm.yaml) | `llm` |
| agentgateway | [`infra/k8s/agents/agentgateway.yaml`](../../../infra/k8s/agents/agentgateway.yaml) | `agentgateway` |
| kagent (Helm values) | [`infra/k8s/agents/kagent-values.yaml`](../../../infra/k8s/agents/kagent-values.yaml) | `kagent` |
| Data Agent + RemoteMCPServer | [`infra/k8s/agents/data-agent.yaml`](../../../infra/k8s/agents/data-agent.yaml) | `kagent` |

## LLM server (bản local thay cho llm-d)

Cấu hình llm-d cần GPU. Ở local dùng **llama.cpp server** (CPU), API kiểu OpenAI,
model **Qwen2.5-1.5B-Instruct Q4_K_M** (file ~1 GB, RAM ~1,3 GB). Lý do chọn model: nhỏ nhất mà vẫn gọi
tool ổn định. llm-d để dành cho khi có GPU.

- `--jinja` bật chat template có tool calling; `--ctx-size 8192`, `--parallel 2` (mỗi slot 4096 token).
- Model tải 1 lần vào PVC `models`; image ghim digest (build b11749).
- `--metrics` mở `/metrics` (token/s, số request) cho Prometheus sau này.
- Hết việc thì `kubectl -n llm scale deploy/qwen --replicas=0` để trả RAM.

Kiểm câu "What is the return rate of SKU sku-00042?" kèm định nghĩa tool →
`finish_reason: tool_calls`, `get_sku_features({"seller_skus": ["sku-00042"]})`.

### Benchmark và tối ưu

Script [`scripts/evidence/bench_llm.py`](../../../scripts/evidence/bench_llm.py): request chat kiểu Agent (system prompt + 3 tool,
~520 token prompt, tối đa 64 token trả lời), 8 request mỗi mức đồng thời, đo gọi thẳng `qwen.llm:8080`.
CPU i5-11400H 6 nhân / 12 luồng.

| Cấu hình | Đồng thời | p50 (s) | p95 (s) | request/s | token ra/s |
|---|---:|---:|---:|---:|---:|
| A: threads 4, parallel 2, ctx 8192, KV f16 | 1 | 2,68 | 2,79 | 0,413 | 21,4 |
| | 2 | 4,71 | 7,87 | 0,381 | 20,1 |
| | 4 | 6,53 | 9,56 | 0,475 | 25,3 |
| **B: threads 6, parallel 4, ctx 16384, flash-attn, KV q8_0** | 1 | 2,62 | 2,92 | 0,413 | 21,2 |
| | 2 | 4,57 | 8,85 | 0,372 | 19,3 |
| | 4 | **6,29** | **7,00** | **0,578** | **29,6** |

- 4 request cùng lúc (Coordinator + 2 Agent + người dùng): B tăng request/s 22%, token/s 17%, giảm p95 27% nhờ 4 slot
  xử lý song song thay vì 2 slot + hàng chờ.
- 1–2 request: như nhau — giải mã bị giới hạn bởi CPU (băng thông RAM), thêm luồng không giúp.
- KV cache q8_0 giữ RAM ở 1,34 GB dù ctx gấp đôi (mỗi slot vẫn 4096 token, đủ cho prompt Agent + tool).
- Đã chọn B ([`llm.yaml`](../../../infra/k8s/agents/llm.yaml)). Số liệu: [`evidence/llm-benchmark-20261021.jsonl`](../../evidence/llm-benchmark-20261021.jsonl).
- Khi có GPU: llm-d; cùng script đo lại để so.

### Đo lại trên EKS

Node `r6i.xlarge` chỉ có 4 vCPU, nhưng cấu hình B đặt 6 luồng (theo laptop 6 nhân). Cùng script, gọi qua
`kubectl port-forward svc/qwen`, chỉ đổi `--threads`:

| Luồng | Đồng thời | p50 (s) | p95 (s) | request/s | token ra/s |
|---|---:|---:|---:|---:|---:|
| 6 (cũ) | 1 | 5,66 | 6,25 | 0,188 | 9,9 |
| | 2 | 8,63 | 14,70 | 0,204 | 10,6 |
| | 4 | 11,02 | 12,60 | 0,342 | 17,7 |
| **3** | 1 | 5,38 | 5,62 | 0,204 | 10,7 |
| | 2 | 7,70 | 14,97 | 0,214 | 11,3 |
| | 4 | **10,96** | **12,44** | **0,345** | **17,8** |
| 4 | 1 | **4,74** | 5,19 | 0,229 | 12,0 |
| | 2 | 7,60 | 13,93 | 0,225 | 11,8 |
| | 4 | 12,57 | 19,14 | 0,269 | 14,0 |

- 6 luồng > 4 vCPU: các luồng tranh nhau CPU, thua hoặc hoà ở mọi mức.
- 4 luồng nhanh nhất khi 1 request, nhưng ở 4 request p95 tăng lên 19,1 s và token/s giảm 21% so với 3 luồng:
  llama.cpp chiếm hết 4 vCPU, tranh với Agent, KEDA, exporter trên cùng node.
- Chọn **3 luồng**: Coordinator gọi 2 Agent song song nên tải thật là 2–4 request; chừa 1 vCPU cho pod khác.
- 8 request mỗi mức ⇒ chênh dưới ~10% coi như hoà. Số liệu: [`evidence/aws/bench-llm-eks-t6-20261030.json`](../../evidence/aws/bench-llm-eks-t6-20261030.json),
  [`t3`](../../evidence/aws/bench-llm-eks-t3-20261030.json), [`t4`](../../evidence/aws/bench-llm-eks-t4-20261030.json).

## agentgateway: cổng cho LLM và MCP

agentgateway v1.6.0 (standalone, cấu hình bằng file), một cổng `:3000`:

| Đường | Tới | Chính sách |
|---|---|---|
| `/v1/chat/completions` | llama.cpp (`provider: custom`, format `completions`) | API key, rate limit 60 request dồn + hồi 1/s |
| `/mcp` (route) | Feature API `/mcp/` | API key, `mcpAuthorization` chỉ cho 4 tool đã duyệt |
| header `x-kagent-host: <agent>.kagent` | Service của Agent (A2A JSON-RPC) | API key, policy `a2a`; đường mà kagent dùng khi bật `proxy.url` |
| `/a2a/<agent>/` | như trên, cho client ngoài cụm | API key, `a2a`, bỏ tiền tố đường dẫn |

- API key nằm trong Secret `agentgateway-keys`, sinh 1 lần bởi script; phía kagent dùng cùng key qua Secret
  `kagent-agw-key` (`AGW_AGENT_KEY` cho model, `AUTH_HEADER` cho MCP).
- Agent không biết địa chỉ thật của LLM hay Feature API; đổi model/backend chỉ sửa gateway.

Kiểm (gọi từ pod trong cụm):

| Thử | Kết quả |
|---|---|
| LLM không có key | 401 |
| LLM có key | 200, trả lời bình thường |
| MCP không có / sai key | 401 |
| MCP có key: `initialize` → `tools/list` → `tools/call get_sku_features` | 200, đúng 1 tool, trả feature |
| 80 request LLM cùng lúc (50 luồng) | **62 × 200, 18 × 429** trong 4,9 s: rate limit có hiệu lực |
| 300 request MCP cùng lúc | 300 × 200: rate limit **không** có hiệu lực ở MCP (bản 1.6.0, đã thử cả khối `mcp:` lẫn route) |
| Agent card qua `/a2a/data-agent/.well-known/agent-card.json` | 200, `url` = `http://agentgateway:3000/a2a/data-agent` |
| A2A không có key | 401 |

A2A trỏ thẳng vào Service của Agent, không qua proxy A2A của kagent controller: card do proxy đó trả về
thiếu trường `url` nên policy `a2a` báo `agent card missing URL` (503).

**Agent gọi Agent qua gateway.** Helm value `proxy.url` của kagent = agentgateway. Khi đó kagent đổi URL
Agent-as-tool `http://data-agent.kagent:8080` thành `http://agentgateway…:3000` và thêm header
`x-kagent-host: data-agent.kagent` (hàm `applyProxyURL` trong `adk_api_translator.go`). Gateway có một route
cho mỗi Agent khớp header đó. Coordinator gửi kèm `Authorization` từ Secret (`headersFrom`), nên A2A cũng
bị chặn nếu thiếu key.

MCP đặt thành route `/mcp` thay vì khối `mcp:` rút gọn: khối rút gọn nhận **mọi** đường dẫn và đứng trước
các route khác, nên request A2A theo header bị nó bắt (lỗi `mcp: client must accept text/event-stream`).

Kiểm card qua header và qua `/a2a/data-agent/` đều 200; MCP không key 401, có key liệt kê tool;
LLM `/v1/models` 200.

Giới hạn còn lại: MCP chưa có rate limit ở gateway; rate limit là bộ đếm trong RAM (không chia sẻ giữa replica).

## kagent và Data Agent

kagent 0.10.3 cài bằng Helm với [`kagent-values.yaml`](../../../infra/k8s/agents/kagent-values.yaml): tắt mọi agent và
tool mẫu (k8s, istio, cilium, grafana-mcp…), chỉ giữ controller, UI, Postgres đi kèm và kmcp (~600 MB RAM).

**Model config dùng chung**: Helm sinh `ModelConfig default-model-config`, provider `OpenAI`,
model `qwen2.5-1.5b-instruct`, `baseUrl` = agentgateway `/v1`, API key từ Secret `kagent-agw-key`.
Mọi Agent tham chiếu cùng tên này; đổi LLM (vd sang llm-d trên GPU) chỉ đổi gateway, không sửa Agent.

**Data Agent** ([`data-agent.yaml`](../../../infra/k8s/agents/data-agent.yaml)): Agent khai báo (Declarative),
tool lấy từ `RemoteMCPServer feature-tools` trỏ vào gateway `/mcp` kèm header `Authorization` đọc từ Secret.
System prompt ngắn, buộc gọi tool thay vì đoán số (model 1.5B dễ bịa).

Kiểm Agent `READY=True ACCEPTED=True`, pod Running; `RemoteMCPServer` báo
`discoveredTools: get_sku_features` (kagent tự gọi `tools/list` qua gateway bằng API key).
Chưa chat thử: LLM đang scale 0 để nhường RAM cho Kubeflow.

Số replica: không khai `replicas` trong Agent ⇒ kagent không ghi đè `spec.replicas` của Deployment
(`manifest_builder.go`: "only set replicas if explicitly specified, so not to override HPA settings"),
để KEDA điều khiển ở bước autoscale.

## Agent 2 (Model Agent) và Coordinator

- [`model-agent.yaml`](../../../infra/k8s/agents/model-agent.yaml): tool `get_sku_features` (Online Feature Store),
  `predict_return_risk` (KServe `return-risk`, protocol v1; model tự đọc feature từ Feast nên tool chỉ gửi
  thông tin đơn), `search_policy_chunks` (RAG). Điểm là tín hiệu xếp hạng, không phải xác suất.
- [`coordinator.yaml`](../../../infra/k8s/agents/coordinator.yaml): hai tool kiểu Agent (data-agent, model-agent),
  gọi qua gateway.
- KEDA: [`data-agent-keda.yaml`](../../../infra/k8s/agents/data-agent-keda.yaml) (min 2, max 4, theo tốc độ request
  A2A ở gateway, đọc từ Prometheus [`prometheus-values.yaml`](../../../infra/k8s/observability/prometheus-values.yaml)).

### Chat thử (A2A `message/send` qua gateway `/a2a/<agent>/`)

| Hỏi | Agent | Tool được gọi | Kết quả |
|---|---|---|---|
| "What are the return features of SKU sku-00002?" | data-agent | `get_sku_features(["sku-00002"])` | đúng số Feast (labeled 0, rate null), 9,7 s |
| "A buyer on tiktok_shop just ordered 3 units of sku-00002 for 30000 cents. What is the return risk?" | model-agent | `predict_return_risk(...)` đúng 4 tham số | score 0,346, abstain (khớp KServe `return-risk` v3), 14 s |
| "Can a TikTok Shop buyer get a refund without sending the item back?" | model-agent | `search_policy_chunks` | trả lời đúng theo chunk "Refund only" |
| "What is the return history of sku-00099?" | coordinator | `kagent__NS__data_agent` | chuyển đúng, trả lời từ data-agent |
| "Return risk of an order of 2 units of sku-00003 on amazon for 4500 cents?" | coordinator | `kagent__NS__model_agent` | score 0,3047 |

Gateway ghi nhận A2A coordinator → agent con qua route theo header (`default/a2a-model-agent`,
`default/a2a-data-agent` trong `agentgateway_requests_total`): Agent không gọi thẳng nhau.

**Giới hạn của model 1,5B (CPU).** Câu hỏi chính sách đi qua 2 Agent (coordinator → model-agent → RAG):
- Ban đầu coordinator tự trả lời, không gọi Agent ("Walmart 90 days" — sai, tài liệu là 30). Đã sửa bằng:
  mô tả Agent bằng tiếng Anh có từ khoá (LLM thấy mô tả Agent như mô tả tool) + system prompt "router,
  bắt buộc gọi đúng 1 tool". Sau sửa: 3/3 câu thử được chuyển đúng Agent.
- model-agent có gọi `search_policy_chunks` nhưng tóm tắt sai (lấy "7–14 days" của mục store drop-off thay vì
  "30 days") hoặc bịa nội dung playbook. Hỏi thẳng model-agent thì đúng.
- Kết luận: kiến trúc (routing, tool, gateway) chạy; chất lượng trả lời nhiều bước bị giới hạn bởi model 1,5B.
  Có GPU thì thay bằng model lớn hơn qua llm-d, chỉ đổi gateway/ModelConfig.

### Autoscale bằng KEDA

Prometheus 29.36.2 (chỉ server) + KEDA 2.20.2. `ScaledObject data-agent`: min 2, max 3 (local), trigger
Prometheus `sum(rate(agentgateway_requests_total{route="default/a2a-data-agent"}[1m]))`, ngưỡng 0,5 request/giây
mỗi pod. Tải 3 request/giây (lấy agent card qua route header) trong 150 s:

| Thời điểm | Tải / pod | Pod chạy |
|---|---|---|
| 0–30 s | 0 → 0,40 | 2 |
| 45 s | 0,82 > 0,5 | **3** (HPA: "above target") |
| hết tải + 5 phút | 0 | **2** (HPA: "All metrics below target") |

Log: [`evidence/keda-data-agent-20261021.txt`](../../evidence/keda-data-agent-20261021.txt). Tải dùng request lấy agent card
(rẻ, không gọi LLM) để đo cơ chế scale mà không làm nghẽn LLM CPU.

Sau khi đo: qwen, model-agent, coordinator, kagent-ui scale 0; ScaledObject data-agent tạm dừng ở 0
(`autoscaling.keda.sh/paused-replicas: "0"`) để trả RAM cho Kubeflow. Bật lại: bỏ annotation, scale các
deployment về 1, `kubectl -n llm scale deploy/qwen --replicas=1`.

## Agent registry (governance)

agentregistry 0.4.0 ([`agentregistry-values.yaml`](../../../infra/k8s/agents/agentregistry-values.yaml),
`scripts/setup/deploy_kind_agentregistry.sh`), DB = database `agentregistry` trên Postgres của pgvector (đỡ một image
Postgres). Đăng bằng `arctl apply` ([`scripts/setup/publish_agentregistry.sh`](../../../scripts/setup/publish_agentregistry.sh),
khai trong [`infra/k8s/agents/registry/`](../../../infra/k8s/agents/registry/feature-tools.yaml)):

| Kind | Tên | Ghi chú |
|---|---|---|
| MCPServer | feature-tools | `remote: streamable-http` = agentgateway `/mcp` (4 tool) |
| Agent | data-agent, model-agent | image `golang-adk:0.10.3`, protocol A2A, `mcpServers: feature-tools` |
| Agent | coordinator | image `golang-adk:0.10.3`, protocol A2A |

Kiểm `arctl get mcpservers` / `arctl get agents` liệt kê đủ 1 + 3. Lỗi đã gặp: MCPServer không cho khai
cả `source` lẫn `remote`; `protocol` phải viết hoa `A2A`; `modelProvider/modelName` đã lỗi thời (chỉ cảnh báo) nên
bỏ, ghi model vào mô tả. Registry không có đăng nhập riêng ⇒ chỉ mở qua ingress có basic auth.

Theo dõi Agent/LLM: [`docs/edai-2/observability.md`](../observability.md).

## System prompt từ Langfuse

Agent lấy system prompt từ công cụ quản lý prompt. Langfuse **v2.95.11**
([`langfuse.yaml`](../../../infra/k8s/agents/langfuse.yaml), ns `langfuse`), chỉ cần Postgres ⇒ dùng database `langfuse` trên
pgvector; v3+ cần thêm ClickHouse/Redis/S3, quá nặng cho máy local.

```
Langfuse prompt platform-<agent> (version, label production)
   │ scripts/setup/sync_agent_prompts.py  (Jenkins: stage "Sync prompts from Langfuse")
   ▼
ConfigMap kagent/agent-prompts (key = tên Agent, annotation = version Langfuse)
   │ Agent.spec.declarative.systemMessageFrom
   ▼
kagent dựng lại config Agent (Secret <agent>/config.json)
```

- Dựng + nạp lần đầu: `bash scripts/setup/deploy_kind_langfuse.sh` (prompt gốc ở
  [`infra/k8s/agents/prompts/`](../../../infra/k8s/agents/prompts/README.md), chỉ tạo khi Langfuse chưa có). Khoá API trong
  Secret `kagent/langfuse-api`; mật khẩu admin UI trong Secret `langfuse/langfuse-env`.
- Kiểm nạp 3 prompt (version 1, production). Tạo `platform-coordinator` **version 2** trên Langfuse (thêm
  "Always answer in English.") → chạy sync → `config.json` của coordinator chứa câu mới sau ~20 s, không cần deploy lại.
- Lỗi đã gặp: 2 pod Langfuse chạy migration Prisma cùng lúc ⇒ migration hỏng (P3009). Sửa: `strategy: Recreate`,
  xoá/tạo lại database trống.
- Local: Langfuse scale 0 khi không dùng.

## Autoscale Coordinator

[`coordinator-keda.yaml`](../../../infra/k8s/agents/coordinator-keda.yaml): min 2, max 3, trigger
`sum(rate(agentgateway_requests_total{route=~"default/a2a-coordinator(-path)?"}[1m]))`, ngưỡng 0,5/pod.
Tải 3 request/giây trong 150 s: **2 → 3 pod** ở giây 45 (0,87/pod), **3 → 2** sau 5 phút hết tải
([evidence](../../evidence/keda-coordinator-20261021.txt)). Sau đo: ScaledObject tạm dừng ở 0 để trả RAM.

## Trên EKS

Ba Agent chạy trên EKS `platform-apps`, cùng dùng Qwen 2.5 1,5B, vào qua `https://agent.<ip>.sslip.io`
(basic auth `seller`, mật khẩu ở Vault `secret/platform/agent-ui`).

![kagent UI: coordinator, data-agent, model-agent cùng model qwen2.5-1.5b-instruct](../../evidence/aws/kagent-ui-agents-20261030.png)

**Chat qua Coordinator.** Câu hỏi: "What is the return rate of SKU APP-00042 on amazon US, and what is the
predicted return risk for an order of 1 unit at 25 USD?". Coordinator gọi data-agent (tool `get_sku_features`) rồi
model-agent (tool `predict_return_risk`) qua A2A trên agentgateway, rồi ghép hai số:

![data-agent trả feature APP-00042: 15306 dòng có nhãn, 2599 Return, tỉ lệ 0.1698](../../evidence/aws/kagent-ui-coordinator-data-agent-20261030.png)

![model-agent gọi predict-return-risk, điểm rủi ro 0.1661](../../evidence/aws/kagent-ui-model-agent-tool-20261030.png)

![Câu trả lời cuối của Coordinator: tỉ lệ 0.1698 và điểm rủi ro 0.1661](../../evidence/aws/kagent-ui-coordinator-answer-20261030.png)

Hai số khớp `/features` (0,1698) và `/predict` (0,166) gọi thẳng Feature API.

**Lỗi tìm thấy khi chạy thật.** Lần chat đầu, câu trả lời ghi `<PHONE_NUMBER>` thay cho tỉ lệ Return:
Feature API trả `0.16980269175486737` (17 chữ số), guardrail `phoneNumber` ở chiều trả lời của agentgateway che
nhầm thành số điện thoại.

![Lần chat đầu: Coordinator trả <PHONE_NUMBER> thay cho tỉ lệ Return](../../evidence/aws/kagent-ui-pii-mask-before-20261030.png)

Sửa ở nguồn: [`serving/app.py`](../../../serving/app.py) làm tròn tỉ lệ và điểm còn 4 số (`Rounded`), test
`test_long_decimals_rounded_for_pii_guardrail`; image `813627b-pii1`, `helm upgrade` revision 2 (rolling).
Activity của data-agent cho thấy cùng câu hỏi trước và sau khi sửa:

![Activity data-agent: trước 0.16980269175486737, sau 0.1698](../../evidence/aws/kagent-ui-rounding-before-after-20261030.png)

Hạn chế đã biết: khi câu hỏi không nêu số lượng/giá, model 1,5B tự điền `ordered_product_net_minor` (có lần
`1000000000000000`, vượt giới hạn API ⇒ tool lỗi, Agent diễn giải sai thành "too little history"). Nêu giá trong câu
hỏi thì đúng.

**KEDA trên EKS.** Cùng cách đo như local: 3 request/giây (agent card, không gọi LLM) vào route `a2a-data-agent`
trong 150 s ([log](../../evidence/aws/keda-data-agent-20261030.txt)):

| Thời điểm | Tải / pod | Pod |
|---|---|---|
| 0–21 s | 0 | 2 |
| 42 s | 933m > 500m | **3** (HPA "above target") |
| 206 s | 45m (hết tải) | 3 |
| 491 s | 0 | **2** (HPA "All metrics below target") |

**Agent registry.** agentregistry trên EKS (`https://registry.<ip>.sslip.io`, basic auth `ops`): 3 Agent đã publish
kèm mô tả, model và nhãn `latest`, cùng 1 MCP server.

![agentregistry: coordinator, data-agent, model-agent](../../evidence/aws/agent-registry-ui-20261030.png)

**Prompt từ Langfuse.** Ba prompt `platform-*` ở nhãn `production`, script seed tạo bằng API rồi đổ vào ConfigMap
`agent-prompts`:

![Langfuse: 3 prompt platform-coordinator, platform-model-agent, platform-data-agent](../../evidence/aws/langfuse-prompts-20261030.png)

Nội dung từng prompt: [coordinator](../../evidence/aws/langfuse-prompt-coordinator-20261030.png),
[model-agent](../../evidence/aws/langfuse-prompt-model-agent-20261030.png),
[data-agent](../../evidence/aws/langfuse-prompt-data-agent-20261030.png).
