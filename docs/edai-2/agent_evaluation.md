# Agent Evaluation trên AWS

EDAI-2, dòng 56: novel idea — Agent eval harness.

Code / cấu hình: Kết quả gate Jenkins và giới hạn tập SKU ghi cùng JSONL bên dưới.

Các kết quả dưới đây thuộc các lần chạy AWS được ghi ngày, không xác nhận dịch vụ đang bật.

## Trên AWS (2026-10-31)

Jenkins trên EC2 `i-0d657089ba4a4ea7e` (`10.42.0.151`) gọi `data-agent` trên EKS context `platform-apps`.
Build `feature-api-agents` **#8 xanh**, gate **12/12 = 1,00 ≥ 0,95**.
Kết quả từng câu: [agent-eval-data-agent-20261031.jsonl](../evidence/aws/agent-eval-data-agent-20261031.jsonl).
Luồng build/push ECR/deploy: [CI/CD AWS](cicd.md#trên-aws-2026-10-31).

![Jenkins feature-api-agents #8: Eval agent xanh sau Deploy Agent](../evidence/aws/jenkins-feature-api-agents-20261031.png)

**Phát hiện PII mask.** Lần đầu chỉ đạt **9/12**: guardrail `phoneNumber` của gateway che cả số đếm bảy chữ số,
ví dụ `6077845` → `<PHONE_NUMBER>`. Câu trả lời mất số chuẩn nên harness chấm trượt.
Lần 12/12 chọn SKU có `labeled_count_90d` dưới bảy chữ số; kết quả chỉ chứng minh tập SKU đó,
không chứng minh đã sửa lỗi mask cho mọi số đếm lớn. Khác với lỗi làm tròn tỷ lệ ngày 30/10,
đây là số đếm nguyên bị nhận nhầm thành số điện thoại.

Coordinator đã Validate / Sync / Deploy xanh nhưng Eval agent thất bại: mỗi câu vượt timeout **180 s**,
do một Qwen 1,5B chạy CPU phục vụ chung các Agent. Chưa có kết quả gate đạt cho Coordinator trên AWS.

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên ngày và môi trường, không dùng thay bằng chứng cloud.

<!-- Mục đích: eval harness cho agent + cổng hồi quy; proof chạy được trên hai nhánh A/B. -->
## ML-N1: eval harness cho agent + cổng hồi quy

Câu hỏi "agent có trả lời **đúng số thật** không?"
không đo được bằng độ trễ hay số token. Harness hỏi agent một bộ câu cố định qua agentgateway (A2A),
lấy **đáp án chuẩn từ Feature API** (chính nguồn agent phải gọi tool), chấm từng câu, rồi dùng độ chính
xác làm cổng chặn trong CI: dưới ngưỡng ⇒ thoát mã 1 ⇒ pipeline đỏ sau bước deploy agent.

| Thành phần | File |
|---|---|
| Harness + chấm điểm | [`ml/agent_eval.py`](../../ml/agent_eval.py), test [`tests/unit/test_agent_eval.py`](../../tests/unit/test_agent_eval.py) |
| Route chấm riêng nhánh B | `a2a-data-agent-b-path` trong [`agentgateway.yaml`](../../infra/k8s/agents/agentgateway.yaml) |
| Kết quả | [`evidence/agent-eval-20261021.jsonl`](../evidence/agent-eval-20261021.jsonl) |

## Luật chấm

Câu "What are the return features of sku-…?" **đạt** khi câu trả lời chứa đủ: `labeled_count_90d`,
`positive_count_90d` (số nguyên đúng) và `return_or_refund_rate_90d` (lệch ≤ 0,005; nhận cả `0.33`, `33%`).
Mã SKU (`sku-00010`) không được tính là số. SKU chưa có lịch sử không chấm (tránh agent nói "0" là đạt).
Lỗi HTTP/timeout của agent = câu trượt, harness không dừng.

**Chuẩn bị dữ liệu**: ảnh chụp online mới nhất (13/10) có mọi số = 0 ⇒ không phân biệt được ai lấy số thật.
Lúc eval, Redis được nạp lại tới mốc 15/03 (Feast không ghi đè giá trị mới bằng giá trị cũ ⇒ phải
`flushall` trước), xong khôi phục về 13/10.

## Proof (2026-10-21, 12 SKU, cùng câu hỏi, cùng tool)

| Agent | Model | Đã đo | p50 |
|---|---|---|---|
| `data-agent` (A) | qwen2.5-1.5b | **12/12 = 100%** | 5,4 s |
| `data-agent-b` (B) | qwen2.5-0.5b | **11/12 = 91,7%** | 3,0 s |

Câu B trượt (sku-00005): nói `labeled_count_90d = 18` sai và tỷ lệ 0,085 không khớp 10/18 — **bịa số**,
loại lỗi mà dashboard độ trễ/token không bao giờ thấy.

Cổng: chạy B với `--min-accuracy 0.95` → `accuracy 0.917` → **exit 1** (bị chặn); A với 0,6 → exit 0.

## Dùng trong CI

Đã nối stage **Eval agent** sau `Deploy Agent` trong `feature-api-agents.Jenkinsfile`,
và sau `Deploy` trong `coordinator.Jenkinsfile` (route `/a2a/coordinator/` hỗ trợ cùng câu hỏi SKU).
Cả hai gọi cùng wrapper; lệnh chạy tại workspace Jenkins:

```bash
## Cần VAULT_ADDR/VAULT_TOKEN + KUBECONFIG; Jenkins bind từ credentials.
bash scripts/run/eval_agent_ci.sh
## Coordinator dùng cùng harness qua route A2A riêng:
EVAL_AGENT=coordinator bash scripts/run/eval_agent_ci.sh
```

Wrapper đọc key từ `vault_env.sh agentgateway`, port-forward Feature API `18000:8000` và
gateway `18300:3000` (đổi bằng `EVAL_FEATURE_PORT`/`EVAL_GATEWAY_PORT` nếu cần), rồi gọi `python3 -m ml.agent_eval` với đủ 12 mã `sku-00001` đến
`sku-00012`, `--min-accuracy 0.95`, `--out out/agent-eval.jsonl`. HTTP chỉ dùng trên loopback
qua port-forward nên không phụ thuộc IP node/cert local của máy phát triển. Khi gọi HTTPS trực tiếp,
harness vẫn kiểm CA như trước.

Jenkins lưu `out/agent-eval.jsonl` và `out/agent-eval-summary.json` bằng `archiveArtifacts`,
kể cả khi chấm dưới ngưỡng. `set -o pipefail` giữ exit 1 qua `tee` ⇒ build đỏ.
Stage này chặn pipeline sau khi đã deploy, **chưa tự rollback agent**.

SKU phải có lịch sử để chấm được. Không có câu chấm được ⇒ accuracy 0 ⇒ build đỏ. CI không
`flushall` hoặc thay dữ liệu Redis đang dùng; người vận hành chuẩn bị fixture có nhãn trước khi chạy.
Bằng chứng A/B ở trên thuộc lần local 2026-10-21. Gate Jenkins đã chạy trên AWS ngày 2026-10-31 ở mục dưới.

</details>

[Mục lục phần này](../README.md) · [Mục lục chung](../../README.md)
