# LLM Inference Platform trên EKS

EDAI-2, dòng 3–6: inference, custom model, agentgateway và benchmark.

llama.cpp trên EKS `platform-apps` tự host Qwen2.5-1.5B-Instruct GGUF Q4_K_M trên CPU, lưu model trên PVC. Đây là model có sẵn tự host, chưa có bằng chứng fine-tuning riêng. Agent gọi API OpenAI-compatible qua agentgateway.

Code: [`llm.yaml`](../../infra/k8s/agents/llm.yaml), [gateway](../../infra/k8s/agents/agentgateway.yaml), [benchmark](../../scripts/evidence/bench_llm.py).

### Đo lại trên EKS, 2026-10-30

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
