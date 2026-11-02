# Coordinator Agent trên EKS

EDAI-2, dòng 25–26: điều phối hai Agent, multi-replica, autoscale và publish.

Coordinator gọi Data Agent và Model Agent qua A2A gateway, tổng hợp feature và prediction. Code: [coordinator.yaml](../../infra/k8s/agents/coordinator.yaml), [KEDA](../../infra/k8s/agents/coordinator-keda.yaml).

## Bằng chứng EKS, 30/10/2026

Câu hỏi APP-00042, Amazon US, 1 sản phẩm giá 25 USD: trả tỉ lệ Return **0,1698** và điểm rủi ro **0,1661**, khớp API.

![Coordinator tổng hợp câu trả lời](../evidence/aws/kagent-ui-coordinator-answer-20261030.png)

Coordinator đã publish trong [Registry](agent_registry.md). Prompt `platform-coordinator`, nhãn `production`, quản lý trên Langfuse và sync vào ConfigMap khi CI deploy.

![Langfuse AWS: prompt của Coordinator và Agent](../evidence/aws/langfuse-prompts-20261030.png)

## Giới hạn

KEDA min 2/max 3 đã cấu hình, thiếu capture test scale trên AWS. CI deploy thành công, eval timeout 180 giây/câu; chưa tính pipeline hoàn tất. [CI/CD](cicd.md).

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
