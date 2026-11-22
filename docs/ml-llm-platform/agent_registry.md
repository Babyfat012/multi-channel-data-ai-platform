# Agent Registry on EKS

The registry and publishing of Agents and the Coordinator.

The registry is a shared directory where Agents and MCP servers are registered (published) so others can find and reuse them.

- Software: agentregistry 0.4.0, running on EKS.
- Registered: 3 Agents (Data Agent, Model Agent, Coordinator) and 1 MCP server.
- UI: `registry.<ip>.sslip.io`, accessed over HTTPS with basic auth (username/password login).
- Each entry has a description, the model in use and a version tag (recorded on EKS: only `latest`, published by hand).

Code: [Helm values](../../infra/k8s/agents/agentregistry-values.yaml), [Data Agent entry](../../infra/k8s/agents/registry/data-agent.yaml), [Model Agent entry](../../infra/k8s/agents/registry/model-agent.yaml), [Coordinator entry](../../infra/k8s/agents/registry/coordinator.yaml), [MCP entry](../../infra/k8s/agents/registry/feature-tools.yaml).

![EKS Registry UI: three published Agents](../evidence/aws/agent-registry-ui-20261030.png)

## Versioning in CI and the gateway gate

agentregistry v0.4.0 versions entries with `metadata.tag` (no `version` field); several tags of one name coexist,
`arctl get agent <name> --all-tags` lists them. CI owns the tag ([`publish_agentregistry`](../../scripts/llm/publish_agentregistry.py)):

```text
Deploy serving API ─► Register candidate ─► Gateway allowlist ─► Deploy Agent ─► Eval agent ─► Promote to stable
                       tag = commit SHA      gateway exposes only        (gateway applied     tag = stable
                       (all 4 entries)       what the SHA declares        only after the gate)   (eval passed)
```

- **Register candidate**: the MCP server and the agents are published under the commit SHA before anything is exposed.
- **Gateway allowlist** ([`check_gateway_allowlist`](../../scripts/llm/check_gateway_allowlist.py)): the gateway config
  stays hand-written ([`agentgateway.yaml`](../../infra/k8s/agents/agentgateway.yaml), A2A routes incl. A/B variants), but
  it is applied only if every agent behind an A2A route is registered under the SHA (`data-agent-b`/`-ab` count as
  `data-agent`) and every tool in `mcpAuthorization` is declared on `feature-tools` with a label
  `tool.platform.dev/<tool>: allowed`. Anything else fails the stage, so the registry decides what the gateway may expose.
- **Promote to stable**: runs only after the agent eval passed (feature-api-agents: MCP server + data/model agents;
  coordinator pipeline: the coordinator).
- Jenkins reaches the registry API through a port-forward (CI RBAC: port-forward only in ns `agentregistry`); the Jenkins
  image ships `arctl` v0.4.0, pinned by checksum.

Checked against the running registry (kind, v0.4.0, dry run): all four tagged manifests are accepted; one label per tool
is required because a label value is limited to 63 characters. Not yet run inside Jenkins.

[Section index](README.md) · [Main index](../README.md)
