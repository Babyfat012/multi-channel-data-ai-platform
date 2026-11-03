# MCP Gateway và A2A Gateway trên EKS

EDAI-2, dòng 5, 23–24: agentgateway và security/policy.

Code: [agentgateway.yaml](../../infra/k8s/agents/agentgateway.yaml), [kagent proxy](../../infra/k8s/agents/kagent-values.yaml).

| Route | Đích | Chính sách / mục đích |
|---|---|---|
| `/v1/chat/completions` | llama.cpp | API key và rate limit kiểm soát inference |
| `/mcp` | Feature API MCP | API key, `mcpAuthorization` giới hạn tool |
| `x-kagent-host` / `/a2a/<agent>/` | Service Agent | API key, policy A2A; gọi Agent qua gateway |

## Vì sao chọn các policy

| Policy | Cách áp dụng và lý do chọn |
|---|---|
| API key `strict` | Áp dụng cho LLM, MCP và A2A; từ chối request thiếu hoặc sai key để kiểm soát client được gọi các dịch vụ qua gateway. Key lấy từ Kubernetes Secret |
| `localRateLimit` cho LLM | Model chạy CPU có năng lực xử lý hữu hạn; giới hạn gọi dồn bằng token bucket cho phép burst 60 request, hồi 1 request/giây. Đây là giới hạn số request, không phải số token của LLM |
| `mcpAuthorization` | Chỉ cho gọi `get_sku_features`, `get_policy_chunks`, `search_policy_chunks`, `predict_return_risk`, đủ cho tra feature, tìm chính sách và dự đoán. Tool ngoài danh sách bị ẩn khỏi `tools/list` và bị chặn khi gọi; thêm tool cần cập nhật danh sách cho phép |
| PII request `reject` | Chặn request khớp mẫu email, thẻ tín dụng hoặc SSN trước khi gửi vào LLM, giảm dữ liệu cá nhân đi vào xử lý. Không chặn mẫu số điện thoại ở request vì số lượng, giá và mã SKU dễ bị nhận nhầm |
| PII response `mask` | Che nội dung khớp mẫu email, số điện thoại hoặc thẻ tín dụng trong câu trả lời, giảm nguy cơ trả dữ liệu cá nhân cho client |
| Policy `a2a` và định tuyến qua gateway | Gom xác thực và định tuyến lời gọi Agent tại gateway; chọn Agent theo `x-kagent-host` hoặc `/a2a/<agent>/`. Policy A2A viết lại URL trong agent card về gateway để các lời gọi tiếp theo tiếp tục đi qua đây |

Các giới hạn của cấu hình hiện hành:

- API key dùng chung cho các Agent, chưa phân quyền riêng từng Agent.
- Bộ đếm rate limit nằm trong RAM của từng replica, không chia sẻ giữa replica. Route MCP hiện không đặt rate limit;
  chưa có bằng chứng giới hạn này có hiệu lực riêng ở MCP.
- PII dựa trên mẫu có thể che nhầm số đếm dài trong câu trả lời: [giới hạn đã ghi nhận](agent_evaluation.md).
  Việc mask không bảo đảm số liệu đầu ra còn đúng; các policy trên mô tả cấu hình hiện có.

## Bằng chứng EKS, 30/10/2026

[Gateway smoke](../evidence/aws/gateway-smoke-20261030.json) đạt **17/17** kiểm tra; [script](../../scripts/evidence/gateway_smoke.py). Grafana ghi nhận route Agent/MCP:

![Grafana AWS: Agent, MCP và gateway](../evidence/aws/grafana-agents-mcp-gateway-20261030.png)

Truy cập bên ngoài: [NGINX/HTTPS](routing_gateway.md).

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
