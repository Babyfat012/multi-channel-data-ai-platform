# Warm Up và Agent Fleet trên EKS

EDAI-2, dòng 27: Substrate phục vụ nhiều Agent với ít worker.

AWS ngày 31/10/2026 dùng Agent Substrate 0.0.9 với kagent 0.10.3, runtime gVisor, WorkerPool `kagent-default`, golden snapshot để khôi phục Agent. Code: [deploy_substrate.sh](../../scripts/setup/deploy_substrate.sh), [manifest Sandbox](../../infra/k8s/agents/sandbox/fleet-agents.yaml).

Ảnh ghi **6 SandboxAgent Ready trên 1 worker**, DESIRED/REPLICAS = 1. Đây là proof mật độ Agent/worker; chưa có phép đo cold start hoặc mức tiết kiệm chi phí.

![EKS: sáu Agent trên một worker](../evidence/aws/substrate-warmup-6-agents-1-worker-20261031.png)

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
