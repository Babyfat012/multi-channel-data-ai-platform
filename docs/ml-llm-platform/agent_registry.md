# Agent Registry on EKS

The registry and publishing of Agents and the Coordinator.

The registry is a shared directory where Agents and MCP servers are registered (published) so others can find and reuse them.

- Software: agentregistry 0.4.0, running on EKS.
- Registered: 3 Agents (Data Agent, Model Agent, Coordinator) and 1 MCP server.
- UI: `registry.<ip>.sslip.io`, accessed over HTTPS with basic auth (username/password login).
- Each entry has a description, the model in use and the version label `latest`.

Code: [Helm values](../../infra/k8s/agents/agentregistry-values.yaml), [Data Agent entry](../../infra/k8s/agents/registry/data-agent.yaml), [Model Agent entry](../../infra/k8s/agents/registry/model-agent.yaml), [Coordinator entry](../../infra/k8s/agents/registry/coordinator.yaml), [MCP entry](../../infra/k8s/agents/registry/feature-tools.yaml).

![EKS Registry UI: three published Agents](../evidence/aws/agent-registry-ui-20261030.png)

[Section index](README.md) · [Main index](../README.md)
