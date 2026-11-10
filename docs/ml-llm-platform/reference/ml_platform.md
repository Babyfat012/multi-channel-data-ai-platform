# ML Platform

ML Platform quản lý feature và vòng đời mô hình dự đoán rủi ro Return hoặc Refund cho Seller.

| Phạm vi | Code / cấu hình | Tài liệu |
|---|---|---|
| Feature offline và online | `feature_repo/`, `infra/k8s/feature-store/` | [Feast](../feature_store.md), [stream feature](../stream_features.md) |
| Training và model registry | `ml/`, `infra/k8s/ml/` | [Training](../training_pipeline.md) |
| Model serving và drift | `ml/`, `infra/k8s/ml/` | [Serving](../ml_inference_service.md), [drift](../drift_monitoring.md) |
| API phục vụ feature và dự đoán | `serving/`, `infra/helm/feature-api/` | [Web API](../web_api_mcp.md) |
| Vận hành dùng chung | `infra/k8s/observability/`, `ci/jenkins/` | [Quan sát](../observability.md), [Vault](../security.md), [CI/CD](../cicd.md) |

[Kiến trúc ML + LLM Platform](../../diagrams/ml-llm-platform-system-architecture-excalidraw.png) · [Evidence ML Platform / LLM Platform](ml_llm_platform_evidence.md).

Feature lấy từ [Data Platform](../../data-platform/reference/data_platform.md); dự đoán và feature được cung cấp cho [LLM Platform](llm_platform.md) qua API công cụ. Hạ tầng vận hành phục vụ cả hai platform.

[Mục lục tài liệu](../../README.md) · [Data & AI Platform](../../../README.md)
