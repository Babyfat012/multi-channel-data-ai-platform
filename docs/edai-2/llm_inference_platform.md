# LLM Inference Platform trên EKS

EDAI-2, dòng 3–6: inference, custom model, agentgateway và benchmark.

Trên cụm EKS `platform-apps`, llama.cpp (một server chạy LLM) tự host model Qwen2.5-1.5B-Instruct bằng CPU.
Model ở định dạng GGUF, nén Q4_K_M (lượng tử hoá 4 bit), và được lưu trên PVC (ổ đĩa gắn cho pod).
Đây là model có sẵn đem về tự host; chưa có bằng chứng fine-tuning riêng. Agent gọi model qua agentgateway bằng API tương thích OpenAI.

**Kết luận:** model chạy ổn trên EKS. Đã đo bằng llm-d-benchmark cả cấu hình baseline lẫn optimized, trên cùng cụm và cùng workload: mỗi cấu hình 42 request, không lỗi.
Khi tải cao (0,4 request/giây), optimized giảm TTFT p50 46% và TTFT p90 40% so với baseline. Khi tải thấp, baseline nhanh hơn vì dùng nhiều luồng CPU hơn.
Một pod LLM phục vụ được khoảng 0,3 request/giây; trên mức đó hệ bị quá tải (chi tiết ở mục benchmark).

Code: [`llm.yaml`](../../infra/k8s/agents/llm.yaml), [gateway](../../infra/k8s/agents/agentgateway.yaml), [benchmark](../../scripts/evidence/bench_llm.py), [llm-d baseline vs optimized](../../scripts/evidence/run_llmd_ab.sh).

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

Manifest tạo namespace `llm`, PVC `models` **3 GiB**, Deployment và Service `qwen` ở cổng 8080.
PVC dùng chế độ `ReadWriteOnce` (mỗi lúc chỉ một node gắn được), nên Deployment chọn chiến lược `Recreate`: pod cũ nhả volume xong thì pod mới mới dùng.
StorageClass `gp3` dùng `WaitForFirstConsumer`, nghĩa là PVC chờ tới khi có pod dùng thì mới bind (gắn với ổ đĩa thật).

Image llama.cpp được ghim cố định bằng image digest.

- Tham số `-hf Qwen/Qwen2.5-1.5B-Instruct-GGUF:Q4_K_M` tự tải model GGUF từ Hugging Face ở lần chạy đầu.
- `LLAMA_CACHE=/models` đặt cache trên PVC, nên khi thay pod thì dùng lại file model đã tải. Pod cần kết nối ra ngoài tới registry và nơi tải model.
- Startup probe cho tối đa khoảng 10 phút để tải và nạp model. Readiness probe kiểm `/health`; chỉ khi đạt thì Service mới chuyển request vào pod.

| Cấu hình hiện hành | Mục đích |
|---|---|
| `--threads 3`, `--parallel 4` | Dùng 3 luồng CPU và 4 slot (4 request được xử lý đồng thời). Con số chọn dựa trên benchmark EKS bên dưới |
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
và tên model ở trên. Cấu hình gateway còn có route MCP, A2A và model nhỏ phục vụ A/B; các route đó chỉ chạy khi có workload
tương ứng. Bước setup này chỉ dựng đường gọi tới Qwen 1,5B; phần kiểm chứng policy xem [Gateway](agent_gateways.md).

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

## Kiểm tra triển khai trên EKS

Trong namespace `llm` trên cluster `platform-apps` có:

- Deployment `qwen` (1,5B) và `qwen-small` (0,5B, nhánh B của A/B test); mỗi deployment có pod ở trạng thái `1/1 Running`.
- PVC `models` 3 GiB và `models-small` 1 GiB, đều `Bound` (gp3). Model nằm trên PVC nên khi pod khởi động lại không phải tải lại.
- Service kiểu `ClusterIP` (chỉ truy cập được trong cụm) ở cổng 8080.

![kubectl get pods,pvc,svc -n llm: qwen và qwen-small Running, PVC models và models-small Bound, Service qwen và qwen-small cổng 8080](../evidence/aws/llm-deploy-pods-pvc-svc.png)

Log khởi động của llama.cpp cho thấy: tải `Qwen2.5-1.5B-Instruct-GGUF:Q4_K_M`, `n_threads = 3`, `n_slots = 4`, rồi `model loaded` và `listening on 0.0.0.0:8080`.
Dòng cảnh báo `no API key is set` đến từ llama.cpp. Server này không tự kiểm API key, nhưng chỉ truy cập được từ trong cụm qua Service `ClusterIP`;
việc kiểm API key do [agentgateway](agent_gateways.md) đảm nhận.

![Log qwen: nạp model Q4_K_M, 3 luồng, 4 slot, model loaded, listening 0.0.0.0:8080](../evidence/aws/llm-qwen-startup-log.png)

## Benchmark bằng llm-d-benchmark

Hai chỉ số chính khi đo LLM:

- **TTFT** (time to first token): thời gian từ lúc gửi request tới khi nhận token đầu tiên.
- **TPOT** (time per output token): thời gian trung bình để sinh mỗi token tiếp theo.

[llm-d-benchmark](https://github.com/llm-d/llm-d-benchmark) có chế độ `run` (run-only): đo một endpoint **đã chạy sẵn**,
không dựng lại cả stack. Bộ đo (harness) `inference-perf` chạy thành pod trong namespace `llm-bench` và gọi `/v1/completions`
vào Service `qwen.llm:8080`. Request dùng streaming nên đo được TTFT. llama.cpp server dùng cùng giao thức OpenAI nên gọi được trực tiếp.

Workload: [`llmd-inference-perf-qwen-cpu.yaml`](../../infra/k8s/benchmark/llmd-inference-perf-qwen-cpu.yaml), lấy từ `sanity_random`
của llm-d-benchmark rồi giảm cỡ cho hợp với CPU: ba mức 0,1 / 0,2 / 0,4 request/giây, mỗi mức 60 s, prompt ~128 token, sinh ~32 token.
Lệnh chạy và cách cài (Python 3.14, commit cố định): [`run_llmd_benchmark.sh`](../../scripts/evidence/run_llmd_benchmark.sh).

**Kiểm tương thích với llama.cpp (local, trước khi chạy EKS).** Tài liệu của llm-d-benchmark và inference-perf không ghi llama.cpp là
engine được hỗ trợ (chỉ ghi vLLM, SGLang, TGI), nên đã thử thật trước ở local:

1. Chạy container llama.cpp cùng image đã ghim như trên EKS. Dùng model Qwen2.5-0.5B Q4_K_M thay cho 1,5B vì chạy trên máy local, nhưng đặt `--alias qwen2.5-1.5b-instruct`.
2. Chạy `inference-perf` (bộ đo lõi mà llm-d-benchmark dùng) với workload của repo, một mức 0,5 request/giây trong 20 s.

| Điểm kiểm | Kết quả |
|---|---|
| `GET /v1/models` | `id` = `qwen2.5-1.5b-instruct` (alias); llm-d-benchmark đối chiếu model theo id này ở bước verify |
| `POST /v1/completions`, streaming, `stream_options.include_usage` | chunk `text_completion`, `choices[0].text`, 24 chunk cho `max_tokens=24`, chunk cuối có `usage` (24 token) |
| `ignore_eos` | `false` dừng ở 3 token, `true` sinh đủ 60 token: llama.cpp nhận tham số này |
| Trường `model` sai tên | vẫn trả kết quả, không bị từ chối |
| Chạy `inference-perf` | 10/10 thành công, 0 lỗi; TTFT TB 0,51 s, TPOT TB 26 ms, request TB 1,32 s; token đầu ra đếm phía client 31,7 so với phía server 32,7 |

Kết quả đầy đủ: [`llmd-inference-perf-llamacpp-local-20261104.json`](../evidence/llmd-inference-perf-llamacpp-local-20261104.json).
Kết luận: llama.cpp đủ tương thích để chạy llm-d-benchmark. Các số trên đo bằng model 0,5B ở máy local, **không phải số EKS**, và không dùng để so với bảng đo bên dưới.

**Chạy thật trên EKS bằng llm-d-benchmark.** Chế độ run-only trỏ vào Service `qwen.llm:8080` (llama.cpp, Qwen2.5-1.5B), harness `inference-perf`, workload
[`llmd-inference-perf-qwen-cpu.yaml`](../../infra/k8s/benchmark/llmd-inference-perf-qwen-cpu.yaml), chạy từ máy quản trị bằng
[`run_llmd_benchmark.sh`](../../scripts/evidence/run_llmd_benchmark.sh). Diễn biến:

1. Công cụ tạo pod `smoketest` để kiểm model; pod đọc `/v1/models` thành công.
2. Công cụ tạo pod harness. Pod tải tokenizer từ Hugging Face rồi chạy lần lượt ba mức tải.
3. Pod kết thúc với `harness_rc: 0` (mã thoát thành công) sau 3 phút 52 giây.

Lần chạy đầu (04/11) chỉ đo cấu hình đã tối ưu. Kết quả đầy đủ trong [llmd-benchmark/](../evidence/aws/llmd-benchmark/summary_lifecycle_metrics.json)
(`run_metadata.yaml`, báo cáo từng mức, `stdout.log`).

### Baseline so với optimized (10/11/2026)

Để thấy phần tối ưu đem lại gì, đo lại bằng llm-d-benchmark **hai cấu hình llama.cpp trên cùng cụm, cùng ngày, cùng workload**, chỉ đổi tham số của Deployment `qwen`:

| Cấu hình | Tham số llama.cpp |
|---|---|
| baseline | cấu hình A ban đầu: `--threads 4`, `--parallel 2`, `--ctx-size 8192`, KV cache f16, `--flash-attn off` |
| optimized | cấu hình hiện hành trong [`llm.yaml`](../../infra/k8s/agents/llm.yaml): `--threads 3`, `--parallel 4`, `--ctx-size 16384`, KV cache `q8_0`, `--flash-attn on` |

Lệnh chạy: [`run_llmd_ab.sh`](../../scripts/evidence/run_llmd_ab.sh). Script patch Deployment về baseline, đo, `apply` lại `llm.yaml`, đo lần nữa,
rồi in bảng bằng [`compare_llmd_runs.py`](../../scripts/evidence/compare_llmd_runs.py).
Baseline chạy lúc 05:09 UTC. Lần đo optimized đầu tiên bị lỗi vì máy quản trị mất kết nối tới API EKS (IP ra Internet không nằm trong danh sách được phép), nên đo lại lúc 06:07 UTC.
Cả hai lần đều `harness_rc: 0`, mỗi lần 42/42 request thành công.
Kết quả: [`baseline-20261110/`](../evidence/aws/llmd-benchmark/baseline-20261110/summary_lifecycle_metrics.json),
[`optimized-20261110/`](../evidence/aws/llmd-benchmark/optimized-20261110/summary_lifecycle_metrics.json).

**Bảng 1. Baseline** (`--threads 4`, `--parallel 2`), [`baseline-20261110/`](../evidence/aws/llmd-benchmark/baseline-20261110/summary_lifecycle_metrics.json):

| Tải (req/s, 60 s) | OK / lỗi | TTFT p50 / p90 (s) | TPOT p50 (ms) | Request p50 / p90 (s) | Đạt (req/s) | Token ra/s |
|---|---:|---:|---:|---:|---:|---:|
| 0,1 | 6 / 0 | 1,79 / 2,37 | 66 | 3,78 / 5,67 | 0,097 | 2,87 |
| 0,2 | 12 / 0 | 1,77 / 2,89 | 95 | 5,44 / 6,59 | 0,189 | 6,09 |
| 0,4 | 24 / 0 | 9,41 / 21,10 | 151 | 14,20 / 25,56 | 0,316 | 9,84 |

**Bảng 2. Optimized** (`--threads 3`, `--parallel 4`, flash-attn, KV `q8_0`), [`optimized-20261110/`](../evidence/aws/llmd-benchmark/optimized-20261110/summary_lifecycle_metrics.json):

| Tải (req/s, 60 s) | OK / lỗi | TTFT p50 / p90 (s) | TPOT p50 (ms) | Request p50 / p90 (s) | Đạt (req/s) | Token ra/s |
|---|---:|---:|---:|---:|---:|---:|
| 0,1 | 6 / 0 | 1,95 / 2,84 | 103 | 5,34 / 8,70 | 0,099 | 2,80 |
| 0,2 | 12 / 0 | 1,97 / 3,88 | 141 | 7,73 / 9,24 | 0,189 | 5,75 |
| 0,4 | 24 / 0 | 5,12 / 12,73 | 253 | 13,50 / 19,52 | 0,335 | 10,61 |

**Bảng 3. Optimized so với baseline** (âm là optimized thấp hơn; với độ trễ thì thấp hơn là tốt hơn):

| Tải (req/s) | TTFT p50 | TTFT p90 | TPOT p50 | Request p90 | Đạt (req/s) | Token ra/s |
|---|---:|---:|---:|---:|---:|---:|
| 0,1 | +9% | +20% | +56% | +53% | +2% | −2% |
| 0,2 | +11% | +34% | +48% | +40% | 0% | −6% |
| 0,4 | **−46%** | **−40%** | +68% | **−24%** | **+6%** | **+8%** |

Đọc kết quả:

- **Ở mức 0,4 request/giây (hệ đã bão hòa), optimized tốt hơn rõ.** So với baseline: TTFT p50 giảm 46% (9,41 s → 5,12 s), TTFT p90 giảm 40% (21,1 s → 12,7 s),
  request p90 giảm 24%, số request xử lý được mỗi giây tăng 6%, số token sinh ra mỗi giây tăng 8%.
  Lý do: baseline chỉ có 2 slot, request thứ ba trở đi phải xếp hàng. Thời gian chờ trong hàng tính vào TTFT, nên TTFT của baseline tăng vọt.
  TPOT của baseline thấp hơn (151 ms so với 253 ms) vì mỗi lúc chỉ 2 request chia CPU, nhưng người dùng vẫn phải chờ lâu hơn để nhận token đầu tiên.
- **Ở mức 0,1–0,2 request/giây, baseline nhanh hơn.** TPOT thấp hơn 33–36%, request p50 nhanh hơn khoảng 30%.
  Lý do: baseline dùng 4 luồng, optimized chỉ 3 luồng (chừa 1 vCPU cho pod khác). Khớp với bảng đo số luồng ở mục dưới: 4 luồng nhanh nhất khi chỉ có 1 request.
- **Vẫn giữ optimized.** Tải thật của hệ có nhiều request chồng nhau: Coordinator gọi 2 Agent song song, thành 2–4 request LLM cùng lúc.
  Với tải này, độ trễ đuôi (p90) và thời gian chờ token đầu quan trọng hơn tốc độ của một request đơn lẻ.
- **Giới hạn của phép so.** Hai cấu hình khác nhau ở nhiều tham số cùng lúc, nên bảng này không tách được tác dụng của từng tham số.
  Phần tách từng tham số nằm ở bảng `bench_llm.py` (laptop: A với B; EKS: 6/3/4 luồng). Mỗi mức tải chỉ có 6–24 request và mỗi cấu hình đo một lần,
  nên không tính khoảng tin cậy. Coi chênh lệch dưới ~10% là hoà.
- Lần chạy optimized ngày 04/11 cho số gần với lần 10/11: ở 0,4 req/s, TTFT p50 là 5,62 s so với 5,12 s, đạt 0,340 so với 0,335 req/s.
  Giới hạn thực tế vẫn vào khoảng 0,3 request/giây cho mỗi pod LLM. Điều này khớp với việc chuỗi Agent gọi LLM nhiều lượt bị chậm khi các request chồng lên nhau
  ([Agent Evaluation](agent_evaluation.md#coordinator-trên-aws-04112026)).

Hai điều chỉnh bắt buộc để llm-d-benchmark chạy được trên cụm EKS này (đã đưa vào script):

1. Dùng `--no-pvc`. Mặc định công cụ tạo PVC `ReadWriteMany` 20 GiB, nhưng EBS `gp3` chỉ hỗ trợ `ReadWriteOnce`, nên báo lỗi `Volume capabilities not supported`.
2. Hạ tài nguyên của pod harness từ mặc định 16 CPU / 32 GiB xuống 1 CPU / 2 GiB. Nếu không, pod bị kẹt ở `Pending` (`Insufficient cpu/memory`) vì node chỉ có 4 vCPU.

Số trong bảng ở mục dưới do một công cụ khác là [`bench_llm.py`](../../scripts/evidence/bench_llm.py) tạo ra, đo với cấu hình khác (số luồng, mức đồng thời), nên không so trực tiếp với các bảng llm-d-benchmark ở trên.

## Đo lại trên EKS

Mục đích: chọn số luồng CPU cho llama.cpp. Node `r6i.xlarge` chỉ có 4 vCPU, nhưng cấu hình B (cấu hình thắng trong [benchmark trên laptop](reference/llm_platform_agents.md), không phải nhánh B của A/B test) đặt 6 luồng, theo laptop 6 nhân.
Chạy cùng một script, gọi qua `kubectl port-forward svc/qwen`, mỗi lần chỉ đổi `--threads`. Cột "Đồng thời" là số request gửi cùng lúc:

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

- 6 luồng nhiều hơn 4 vCPU nên các luồng tranh nhau CPU; kết quả thua hoặc hoà ở mọi mức.
- 4 luồng nhanh nhất khi chỉ có 1 request. Nhưng khi có 4 request, p95 tăng lên 19,1 s và token/s giảm 21% so với 3 luồng.
  Lý do: llama.cpp chiếm hết 4 vCPU, tranh CPU với Agent, KEDA và exporter chạy trên cùng node.
- Chọn **3 luồng**: Coordinator gọi 2 Agent song song nên tải thật là 2–4 request cùng lúc; chừa lại 1 vCPU cho các pod khác.
- Mỗi mức chỉ có 8 request, nên chênh lệch dưới ~10% được coi như hoà. Số liệu: [`evidence/aws/bench-llm-eks-t6-20261030.json`](../evidence/aws/bench-llm-eks-t6-20261030.json),
  [`t3`](../evidence/aws/bench-llm-eks-t3-20261030.json), [`t4`](../evidence/aws/bench-llm-eks-t4-20261030.json).



[Policy và proof Gateway](agent_gateways.md).

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
