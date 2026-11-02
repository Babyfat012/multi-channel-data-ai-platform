# Global Model Config

EDAI-2, dòng 7: cấu hình model dùng chung.

`default-model-config` dùng provider `OpenAI`, model `qwen2.5-1.5b-instruct`, `baseUrl` trỏ tới agentgateway `/v1`, key từ Secret `kagent-agw-key`. Ba Agent cùng tham chiếu ModelConfig; gateway chuyển request tới llama.cpp EKS.

Code: [Helm values](../../infra/k8s/agents/kagent-values.yaml), [Data Agent](../../infra/k8s/agents/data-agent.yaml), [Model Agent](../../infra/k8s/agents/model-agent.yaml), [Coordinator](../../infra/k8s/agents/coordinator.yaml).

Ảnh EKS ngày 30/10/2026 thể hiện ba Agent dùng chung model:

![kagent UI EKS: ba Agent dùng qwen2.5-1.5b-instruct](../evidence/aws/kagent-ui-agents-20261030.png)

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
