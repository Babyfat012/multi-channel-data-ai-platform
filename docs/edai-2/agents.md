# Data Agent và Model Agent trên EKS

EDAI-2, dòng 19–22: MCP, multi-replica, autoscale, Sandbox và publish.

Data Agent lấy feature SKU qua MCP. Model Agent dùng online feature, gọi KServe để dự đoán và RAG để tìm chính sách trả hàng. Các lời gọi đi qua [Agent Gateways](agent_gateways.md), cùng [Model Config](global_model_config.md).

Code: [Data Agent](../../infra/k8s/agents/data-agent.yaml), [Model Agent](../../infra/k8s/agents/model-agent.yaml), [KEDA Data Agent](../../infra/k8s/agents/data-agent-keda.yaml), [KEDA Model Agent](../../infra/k8s/agents/model-agent-keda.yaml).

## Bằng chứng EKS, 30–31/10/2026

APP-00042 có 15.306 dòng gắn nhãn, 2.599 Return, tỉ lệ 0,1698. Model Agent gọi prediction tool, score 0,1661; số khớp API trực tiếp.

![Data Agent lấy feature](../evidence/aws/kagent-ui-coordinator-data-agent-20261030.png)

![Model Agent gọi prediction tool](../evidence/aws/kagent-ui-model-agent-tool-20261030.png)

![Model Agent trả lời theo chính sách RAG](../evidence/aws/rag-model-agent-answer-20261030.png)

KEDA Data Agent: 3 request/giây vào agent card trong 150 giây, pod **2 → 3 → 2**. Script [`keda_scale_test.sh`](../../scripts/evidence/keda_scale_test.sh) ghi thêm tên từng pod ở mỗi mốc, và [`eks_runtime_proof.sh`](../../scripts/evidence/eks_runtime_proof.sh) in ScaledObject/HPA, từng pod Agent chạy ở node nào, WorkerPool và SandboxAgent (04/11/2026, [output](../evidence/aws/eks-runtime-proof-part2.png)). Đây là tải route A2A, không phải tải inference LLM. [Log EKS](../evidence/aws/keda-data-agent-20261030.txt).

### KEDA Model Agent, 04/11/2026 (EKS `platform-apps`)

[ScaledObject](../../infra/k8s/agents/model-agent-keda.yaml) min 2, max 3, ngưỡng 0,5 request/giây mỗi pod theo route `a2a-model-agent`.
Áp dụng bằng `kubectl apply`, rồi chạy [`keda_scale_test.sh`](../../scripts/evidence/keda_scale_test.sh) (3 request/giây lấy agent card, 150 giây).
Log: [keda-model-agent.txt](../evidence/aws/keda-model-agent.txt): pod **2 → 3 → 2** (lên 3 sau khoảng 27 giây khi tải vượt ngưỡng, về 2 sau khoảng 8 phút
do HPA chờ ổn định). Tải là A2A lấy agent card, không phải tải inference LLM.

![ScaledObject và HPA Model Agent sau khi áp dụng: READY True, min 2, max 3, 2 pod Running](../evidence/aws/keda-model-agent-apply.png)

![Lúc tải: HPA Model Agent và Coordinator đều REPLICAS 3, mỗi pod một dòng](../evidence/aws/keda-scale-up-model-agent-coordinator.png)

![Sau khi tải dừng: cả hai về REPLICAS 2](../evidence/aws/keda-scale-down-model-agent-coordinator.png)

Vị trí pod: `podAntiAffinity` chỉ là ưu tiên. Hai pod đầu của Model Agent cùng nằm trên một node vì node kia đã đặt gần hết CPU;
pod thứ ba lên node còn lại, và lúc về 2 HPA giữ lại hai pod ở hai node khác nhau.

Sandbox dùng Agent Substrate/gVisor; Agent gọi được `get-sku-features`. Model CPU 1,5B chép `830847` thành `83,084`; ảnh chứng minh đường gọi tool trong Sandbox.

![Sandbox Agent gọi tool trên EKS](../evidence/aws/substrate-sandbox-agent-ui-20261031.png)

Trạng thái trên EKS (04/11/2026): `workerpool kagent-default` DESIRED 1, REPLICAS 1; sáu SandboxAgent (`data-agent-sandbox` và năm `fleet-*`) đều `READY True`, `ACCEPTED True`;
các pod Substrate trong `ate-system` (api server, controller, hai `atelet`, `atenet-router`, DNS, rustfs, sáu `valkey-cluster`) đều Running.
Kubernetes không có RuntimeClass nào (`No resources found`): gVisor do Substrate áp dụng qua `sandboxClass` của WorkerPool và image `ateom-gvisor`, không qua RuntimeClass.

![WorkerPool kagent-default: sandboxClass gvisor, ateomImage ateom-gvisor v0.0.9, 1 replica](../evidence/aws/workerpool-gvisor.png)

![Output eks_runtime_proof.sh phần 2: ScaledObject/HPA, pod từng Agent theo node, WorkerPool, sáu SandboxAgent Ready, pod Substrate](../evidence/aws/eks-runtime-proof-part2.png)

Code Sandbox: [deploy_substrate.sh](../../scripts/setup/deploy_substrate.sh), [manifest](../../infra/k8s/agents/sandbox/fleet-agents.yaml).

## Giới hạn

Khi câu hỏi thiếu giá/số lượng, model nhỏ có thể tự điền tham số sai. [Registry](agent_registry.md) có proof publish; [Agent Evaluation](agent_evaluation.md) ghi giới hạn PII và kết quả chấm.

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
