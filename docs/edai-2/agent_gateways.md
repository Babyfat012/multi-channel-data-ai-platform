# MCP Gateway và A2A Gateway trên EKS

EDAI-2, dòng 5, 23–24: agentgateway và security/policy.

Code: [agentgateway.yaml](../../infra/k8s/agents/agentgateway.yaml), [kagent proxy](../../infra/k8s/agents/kagent-values.yaml).

| Route | Đích | Chính sách / mục đích |
|---|---|---|
| `/v1/chat/completions` | llama.cpp | API key và rate limit kiểm soát inference |
| `/mcp` | Feature API MCP | API key, `mcpAuthorization` giới hạn tool |
| `x-kagent-host` / `/a2a/<agent>/` | Service Agent | API key, policy A2A; gọi Agent qua gateway |

Key lấy từ Kubernetes Secret. PII có thể che nhầm số đếm dài: [giới hạn](agent_evaluation.md). Bộ đếm rate limit trong RAM không chia sẻ giữa replica; chưa có bằng chứng rate limit riêng ở MCP.

## Bằng chứng EKS, 30/10/2026

[Gateway smoke](../evidence/aws/gateway-smoke-20261030.json) đạt **17/17** kiểm tra; [script](../../scripts/evidence/gateway_smoke.py). Grafana ghi nhận route Agent/MCP:

![Grafana AWS: Agent, MCP và gateway](../evidence/aws/grafana-agents-mcp-gateway-20261030.png)

Truy cập bên ngoài: [NGINX/HTTPS](routing_gateway.md).

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
