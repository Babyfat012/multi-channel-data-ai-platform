# Agent Registry trên EKS

EDAI-2, dòng 8, 21 và 26: registry và publish Agent/Coordinator.

Ngày 30/10/2026, agentregistry 0.4.0 trên EKS có 3 Agent và 1 MCP server; UI `registry.<ip>.sslip.io` qua HTTPS/basic auth. Entry có mô tả, model và nhãn `latest`.

Code: [Helm values](../../infra/k8s/agents/agentregistry-values.yaml), [Data Agent entry](../../infra/k8s/agents/registry/data-agent.yaml), [Model Agent entry](../../infra/k8s/agents/registry/model-agent.yaml), [Coordinator entry](../../infra/k8s/agents/registry/coordinator.yaml), [MCP entry](../../infra/k8s/agents/registry/feature-tools.yaml).

![Registry UI EKS: ba Agent đã publish](../evidence/aws/agent-registry-ui-20261030.png)

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
