# Agent Registry trên EKS

EDAI-2, dòng 8, 21 và 26: registry và publish Agent/Coordinator.

Registry là danh bạ chung, nơi các Agent và MCP server được đăng ký (publish) để người khác tìm và dùng lại.

- Phần mềm: agentregistry 0.4.0, chạy trên EKS.
- Đã đăng ký: 3 Agent (Data Agent, Model Agent, Coordinator) và 1 MCP server.
- Giao diện: `registry.<ip>.sslip.io`, truy cập qua HTTPS và basic auth (đăng nhập bằng tên/mật khẩu).
- Mỗi entry (mục đăng ký) có mô tả, model đang dùng và nhãn phiên bản `latest`.

Code: [Helm values](../../infra/k8s/agents/agentregistry-values.yaml), [Data Agent entry](../../infra/k8s/agents/registry/data-agent.yaml), [Model Agent entry](../../infra/k8s/agents/registry/model-agent.yaml), [Coordinator entry](../../infra/k8s/agents/registry/coordinator.yaml), [MCP entry](../../infra/k8s/agents/registry/feature-tools.yaml).

![Registry UI EKS: ba Agent đã publish](../evidence/aws/agent-registry-ui-20261030.png)

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
