# Warm Up và Agent Fleet trên EKS

EDAI-2, dòng 27: Substrate phục vụ nhiều Agent với ít worker.

Cấu hình trên AWS:
- Agent Substrate 0.0.9 chạy cùng kagent 0.10.3.
- Runtime gVisor (sandbox cô lập cho từng Agent).
- WorkerPool `kagent-default` (nhóm worker dùng chung cho các Agent).
- Golden snapshot: ảnh chụp sẵn trạng thái Agent, dùng để khôi phục Agent.

Code: [deploy_substrate.sh](../../scripts/setup/deploy_substrate.sh), [manifest Sandbox](../../infra/k8s/agents/sandbox/fleet-agents.yaml).

Ảnh dưới ghi **6 SandboxAgent ở trạng thái Ready trên cùng 1 worker**, với DESIRED/REPLICAS = 1. Đây là bằng chứng về
mật độ (nhiều Agent trên một worker). Chưa có phép đo thời gian khởi động nguội (cold start) hay mức tiết kiệm chi phí.

![EKS: sáu Agent trên một worker](../evidence/aws/substrate-warmup-6-agents-1-worker-20261031.png)

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
