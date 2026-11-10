# Agent Fleet and Warm Pool on EKS

Agent Substrate serves many Agents with few workers.

AWS configuration:
- Agent Substrate 0.0.9 running with kagent 0.10.3.
- gVisor runtime (an isolated sandbox per Agent).
- WorkerPool `kagent-default` (a worker pool shared by the Agents).
- Golden snapshot: a pre-captured Agent state, used to restore Agents.

Code: [deploy_substrate.sh](../../scripts/setup/deploy_substrate.sh), [Sandbox manifest](../../infra/k8s/agents/sandbox/fleet-agents.yaml).

The screenshot below shows **6 SandboxAgents in Ready state on the same single worker**, with DESIRED/REPLICAS = 1. This is evidence of
density (many Agents on one worker). There is no measurement of cold start time or cost savings yet.

![EKS: six Agents on one worker](../evidence/aws/substrate-warmup-6-agents-1-worker-20261031.png)

[Section index](README.md) · [Main index](../README.md)
