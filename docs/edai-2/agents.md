# Data Agent và Model Agent trên EKS

EDAI-2, dòng 19–22: MCP, multi-replica, autoscale, Sandbox và publish.

Data Agent lấy feature SKU qua MCP. Model Agent dùng online feature, gọi KServe để dự đoán và RAG để tìm chính sách trả hàng. Các lời gọi đi qua [Agent Gateways](agent_gateways.md), cùng [Model Config](global_model_config.md).

Code: [Data Agent](../../infra/k8s/agents/data-agent.yaml), [Model Agent](../../infra/k8s/agents/model-agent.yaml), [KEDA Data Agent](../../infra/k8s/agents/data-agent-keda.yaml).

## Bằng chứng EKS, 30–31/10/2026

APP-00042 có 15.306 dòng gắn nhãn, 2.599 Return, tỉ lệ 0,1698. Model Agent gọi prediction tool, score 0,1661; số khớp API trực tiếp.

![Data Agent lấy feature](../evidence/aws/kagent-ui-coordinator-data-agent-20261030.png)

![Model Agent gọi prediction tool](../evidence/aws/kagent-ui-model-agent-tool-20261030.png)

![Model Agent trả lời theo chính sách RAG](../evidence/aws/rag-model-agent-answer-20261030.png)

KEDA Data Agent: 3 request/giây vào agent card trong 150 giây, pod **2 → 3 → 2**. Đây là tải route A2A, không phải tải inference LLM. [Log EKS](../evidence/aws/keda-data-agent-20261030.txt).

Sandbox dùng Agent Substrate/gVisor; Agent gọi được `get-sku-features`. Model CPU 1,5B chép `830847` thành `83,084`; ảnh chứng minh đường gọi tool trong Sandbox.

![Sandbox Agent gọi tool trên EKS](../evidence/aws/substrate-sandbox-agent-ui-20261031.png)

Code Sandbox: [deploy_substrate.sh](../../scripts/setup/deploy_substrate.sh), [manifest](../../infra/k8s/agents/sandbox/fleet-agents.yaml).

## Giới hạn

Model Agent chưa có ScaledObject và proof KEDA riêng trên AWS. Khi câu hỏi thiếu giá/số lượng, model nhỏ có thể tự điền tham số sai. [Registry](agent_registry.md) có proof publish; [Agent Evaluation](agent_evaluation.md) ghi giới hạn PII và kết quả chấm.

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
