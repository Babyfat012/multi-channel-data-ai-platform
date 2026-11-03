# LLM Inference Platform trên EKS

EDAI-2, dòng 3–6: inference, custom model, agentgateway và benchmark.

llama.cpp trên EKS `platform-apps` tự host Qwen2.5-1.5B-Instruct GGUF Q4_K_M trên CPU, lưu model trên PVC. Đây là model có sẵn tự host, chưa có bằng chứng fine-tuning riêng. Agent gọi API OpenAI-compatible qua agentgateway.

Code: [`llm.yaml`](../../infra/k8s/agents/llm.yaml), [gateway](../../infra/k8s/agents/agentgateway.yaml), [benchmark](../../scripts/evidence/bench_llm.py).

## Cách setup trên EKS

Các lệnh dưới đây chạy từ gốc repo, khi EKS `platform-apps` đã có worker, kubeconfig và EBS CSI.
Chuẩn bị StorageClass `gp3` mặc định theo [Cloud setup](../shared/cloud_setup.md#3-kubernetes-cho-ml-platform--llm-platform);
PVC trong `llm.yaml` dùng StorageClass mặc định của cụm.

### 1. Dựng PVC và llama.cpp

```bash
export KCTX=platform-apps
kubectl --context "$KCTX" get nodes
kubectl --context "$KCTX" get storageclass
kubectl --context "$KCTX" apply -f infra/k8s/agents/llm.yaml
kubectl --context "$KCTX" -n llm rollout status deployment/qwen --timeout=15m
kubectl --context "$KCTX" -n llm get pvc models
kubectl --context "$KCTX" -n llm logs deployment/qwen --tail=50
```

Manifest tạo namespace `llm`, PVC `models` **3 GiB**, Deployment và Service `qwen` cổng 8080.
PVC dùng `ReadWriteOnce`; Deployment chọn `Recreate` để pod cũ nhả volume trước khi pod mới dùng.
Với `gp3` dùng `WaitForFirstConsumer`, PVC chờ có pod sử dụng rồi mới bind.

Container llama.cpp được cố định bằng image digest. Tham số
`-hf Qwen/Qwen2.5-1.5B-Instruct-GGUF:Q4_K_M` tự tải model GGUF có sẵn từ Hugging Face ở lần chạy đầu;
`LLAMA_CACHE=/models` đặt cache trên PVC để dùng lại sau khi thay pod. Cần kết nối ra registry và nơi tải model.
Startup probe cho phép tối đa khoảng 10 phút để tải và nạp model; readiness probe kiểm `/health` trước khi Service chuyển request.

| Cấu hình hiện hành | Mục đích |
|---|---|
| `--threads 3`, `--parallel 4` | Dùng 3 luồng CPU và 4 slot xử lý đồng thời; lựa chọn dựa trên benchmark EKS bên dưới |
| `--ctx-size 16384` | Context tổng 16.384 token, tương ứng 4.096 token mỗi slot với cấu hình 4 slot |
| `--flash-attn on`, KV cache `q8_0` | Giảm bộ nhớ KV cache so với cấu hình mặc định để phục vụ context đã chọn |
| `--jinja`, `--alias qwen2.5-1.5b-instruct` | Bật chat template hỗ trợ tool calling và đặt tên model cho client/gateway |
| `--metrics` | Mở `/metrics` cho Prometheus; pod có annotation scrape cổng 8080 |

### 2. Kiểm tra API của LLM

Mở port-forward trong một terminal:

```bash
kubectl --context "$KCTX" -n llm port-forward svc/qwen 18080:8080
```

Trong terminal khác, kiểm tra health và gửi một câu chat:

```bash
curl --fail-with-body http://127.0.0.1:18080/health
curl --fail-with-body http://127.0.0.1:18080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen2.5-1.5b-instruct","messages":[{"role":"user","content":"Reply with OK."}],"max_tokens":16,"temperature":0}'
```

Kết quả mong đợi: health trả HTTP 200; chat trả JSON có `choices` và nội dung trả lời.

### 3. Nối agentgateway

[`agentgateway.yaml`](../../infra/k8s/agents/agentgateway.yaml) khai báo model `qwen2.5-1.5b-instruct`
với `baseUrl: http://qwen.llm.svc.cluster.local:8080/v1`. Gateway nhận API key qua Secret
`agentgateway-keys`, trường `AGW_AGENT_KEY`. Khi dựng mới, tạo key một lần; nếu Secret đã có thì giữ nguyên:

```bash
kubectl --context "$KCTX" apply -f infra/k8s/agents/agentgateway.yaml
if ! kubectl --context "$KCTX" -n agentgateway get secret agentgateway-keys >/dev/null; then
  agw_key="agw_$(openssl rand -hex 16)"
  kubectl --context "$KCTX" -n agentgateway create secret generic agentgateway-keys \
    --from-literal=AGW_AGENT_KEY="$agw_key"
  unset agw_key
fi
kubectl --context "$KCTX" -n agentgateway rollout restart deployment/agentgateway
kubectl --context "$KCTX" -n agentgateway rollout status deployment/agentgateway --timeout=180s
```

Agent gọi gateway tại `http://agentgateway.agentgateway.svc.cluster.local:3000/v1`, dùng cùng API key
và tên model trên. Cấu hình gateway còn có route MCP, A2A và model nhỏ phục vụ A/B; các nhánh đó cần workload
tương ứng. Bước setup này dựng đường gọi Qwen 1,5B; kiểm chứng policy xem [Gateway](agent_gateways.md).

Để kiểm tra đường gọi qua gateway, mở port-forward trong một terminal:

```bash
kubectl --context "$KCTX" -n agentgateway port-forward svc/agentgateway 13000:3000
```

Trong terminal khác, đặt `KCTX=platform-apps`, đọc key từ Secret và gọi đúng model 1,5B:

```bash
agw_key=$(kubectl --context "$KCTX" -n agentgateway get secret agentgateway-keys \
  -o jsonpath='{.data.AGW_AGENT_KEY}' | base64 -d)
curl --fail-with-body http://127.0.0.1:13000/v1/chat/completions \
  -H "Authorization: Bearer $agw_key" -H 'Content-Type: application/json' \
  -d '{"model":"qwen2.5-1.5b-instruct","messages":[{"role":"user","content":"Reply with OK."}],"max_tokens":16,"temperature":0}'
unset agw_key
```

Các bước trên là hướng dẫn tái lập từ cấu hình repo. Số liệu đã đo trên AWS nằm ở phần dưới.

## Đo lại trên EKS, 2026-10-30

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
- 8 request mỗi mức ⇒ chênh dưới ~10% coi như hoà. Số liệu: [`evidence/aws/bench-llm-eks-t6-20261030.json`](../evidence/aws/bench-llm-eks-t6-20261030.json),
  [`t3`](../evidence/aws/bench-llm-eks-t3-20261030.json), [`t4`](../evidence/aws/bench-llm-eks-t4-20261030.json).



[Policy và proof Gateway](agent_gateways.md).

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
