# ML Platform

ML Platform quản lý feature và vòng đời mô hình dự đoán rủi ro Return hoặc Refund cho Seller.

| Phạm vi | Code / cấu hình | Tài liệu |
|---|---|---|
| Feature offline và online | `feature_repo/`, `infra/k8s/feature-store/` | [Feast](ml-platform-feature-store.md), [stream feature](ml-platform-stream-feature.md) |
| Training và model registry | `ml/`, `infra/k8s/ml/` | [Training](ml-platform-training.md) |
| Model serving và drift | `ml/`, `infra/k8s/ml/` | [Serving](ml-platform-serving.md), [drift](ml-platform-drift.md) |
| API phục vụ feature và dự đoán | `serving/`, `infra/helm/feature-api/` | [Web API](ml-llm-platform-web-api.md) |
| Vận hành dùng chung | `infra/k8s/observability/`, `ci/jenkins/` | [Quan sát](ml-llm-platform-observability.md), [Vault](ml-llm-platform-vault.md), [CI/CD](ml-llm-platform-cicd.md) |

[Kiến trúc ML + LLM Platform](diagrams/ml-llm-platform-system-architecture.html) · [Evidence ML Platform / LLM Platform](ml-llm-platform-evidence.md).

Feature lấy từ [Data Platform](data-platform.md); dự đoán và feature được cung cấp cho [LLM Platform](llm-platform.md) qua API công cụ. Hạ tầng vận hành phục vụ cả hai platform.

[Mục lục tài liệu](README.md) · [Data & AI Platform](../README.md)
