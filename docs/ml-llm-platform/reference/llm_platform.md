# LLM Platform

The LLM Platform provides conversation for the Seller through an LLM, RAG and agents that use the system's data and models.

| Scope | Code / config | Docs |
|---|---|---|
| LLM inference, agents and orchestration | `infra/k8s/agents/` | [Agents](llm_platform_agents.md) |
| Documents, embeddings and retrieval | `rag/`, `dags/ml_rag_ingest.py` | [RAG](../rag_pipeline.md) |
| Gateway and tool access | `infra/k8s/agents/`, `serving/` | [Gateway](../routing_gateway.md), [API](../web_api_mcp.md) |
| Evaluation and A/B | `ml/`, `infra/k8s/agents/` | [Eval](../agent_evaluation.md), [A/B](../ab_testing.md) |
| Shared operations | `infra/k8s/observability/`, `ci/jenkins/` | [Observability](../observability.md), [Vault](../security.md), [CI/CD](../cicd.md) |

[ML + LLM Platform architecture](../../diagrams/ml-llm-platform-system-architecture-excalidraw.png) · [ML Platform / LLM Platform evidence](ml_llm_platform_evidence.md).

Agents query figures from the [Data Platform](../../data-platform/reference/data_platform.md), and features and predictions from the [ML Platform](ml_platform.md). The API and shared operations infrastructure are referenced from both platforms.

[Docs index](../../README.md) · [Data & AI Platform](../../../README.md)
