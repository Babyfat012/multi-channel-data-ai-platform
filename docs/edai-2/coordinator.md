# Coordinator Agent trên EKS

EDAI-2, dòng 25–26: điều phối hai Agent, multi-replica, autoscale và publish.

Coordinator gọi Data Agent và Model Agent qua A2A gateway, tổng hợp feature và prediction. Code: [coordinator.yaml](../../infra/k8s/agents/coordinator.yaml), [KEDA](../../infra/k8s/agents/coordinator-keda.yaml).

## Bằng chứng EKS, 30/10/2026

Câu hỏi APP-00042, Amazon US, 1 sản phẩm giá 25 USD: trả tỉ lệ Return **0,1698** và điểm rủi ro **0,1661**, khớp API.

![Coordinator tổng hợp câu trả lời](../evidence/aws/kagent-ui-coordinator-answer-20261030.png)

Coordinator đã publish trong [Registry](agent_registry.md). Prompt `platform-coordinator`, nhãn `production`, quản lý trên Langfuse và sync vào ConfigMap khi CI deploy.

![Langfuse AWS: prompt của Coordinator và Agent](../evidence/aws/langfuse-prompts-20261030.png)

## Autoscale trên EKS, 04/11/2026

[KEDA Coordinator](../../infra/k8s/agents/coordinator-keda.yaml) min 2, max 3, ngưỡng 0,5 request/giây mỗi pod theo route `a2a-coordinator`.
Chạy [`keda_scale_test.sh`](../../scripts/evidence/keda_scale_test.sh), log [keda-coordinator.txt](../evidence/aws/keda-coordinator.txt): pod **2 → 3 → 2**
(lên 3 sau khoảng 55 giây, về 2 sau khoảng 8 phút). Cùng bài chạy với Model Agent; ảnh pod ở [Agents](agents.md#keda-model-agent-04112026-eks-platform-apps).
Phần `Events` trong log có một cảnh báo `FailedGetExternalMetric` cũ (hàng chục nghìn lần trong 2 ngày): lúc đó node EKS đang scale về 0 nên API metric
của KEDA không trả lời; cảnh báo không xuất hiện trong lần scale này (các sự kiện `SuccessfulRescale` bên dưới là của lần chạy).

![HPA Coordinator và Model Agent đều REPLICAS 3 khi có tải](../evidence/aws/keda-scale-up-model-agent-coordinator.png)

![Sau khi tải dừng, HPA Coordinator và Model Agent về REPLICAS 2](../evidence/aws/keda-scale-down-model-agent-coordinator.png)

## Giới hạn

CI deploy thành công, eval timeout 180 giây/câu; chưa tính pipeline hoàn tất. [CI/CD](cicd.md).

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
