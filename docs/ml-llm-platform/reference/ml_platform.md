# ML Platform

The ML Platform manages features and the lifecycle of the model that predicts Return-or-Refund risk for the Seller.

| Scope | Code / config | Docs |
|---|---|---|
| Offline and online features | `feature_repo/`, `infra/k8s/feature-store/` | [Feast](../feature_store.md), [stream features](../stream_features.md) |
| Training and model registry | `ml/`, `infra/k8s/ml/` | [Training](../training_pipeline.md) |
| Model serving and drift | `ml/`, `infra/k8s/ml/` | [Serving](../ml_inference_service.md), [drift](../drift_monitoring.md) |
| API serving features and predictions | `serving/`, `infra/helm/feature-api/` | [Web API](../web_api_mcp.md) |
| Shared operations | `infra/k8s/observability/`, `ci/jenkins/` | [Observability](../observability.md), [Vault](../security.md), [CI/CD](../cicd.md) |

[ML + LLM Platform architecture](../../diagrams/ml-llm-platform-system-architecture-excalidraw.png) · [ML Platform / LLM Platform evidence](ml_llm_platform_evidence.md).

Features come from the [Data Platform](../../data-platform/reference/data_platform.md); predictions and features are provided to the [LLM Platform](llm_platform.md) through the tool API. The operations infrastructure serves both platforms.

[Docs index](../../README.md) · [Data & AI Platform](../../../README.md)
