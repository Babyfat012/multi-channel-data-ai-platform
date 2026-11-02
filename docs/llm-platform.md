# LLM Platform

LLM Platform cung cấp hội thoại cho Seller bằng LLM, RAG và agent sử dụng dữ liệu, mô hình của hệ thống.

| Phạm vi | Code / cấu hình | Tài liệu |
|---|---|---|
| LLM inference, agent và điều phối | `infra/k8s/agents/` | [Agent](llm-platform-agents.md) |
| Tài liệu, embedding và truy xuất | `rag/`, `dags/ml_rag_ingest.py` | [RAG](llm-platform-rag.md) |
| Gateway và truy cập công cụ | `infra/k8s/agents/`, `serving/` | [Gateway](llm-platform-gateway.md), [API](ml-llm-platform-web-api.md) |
| Đánh giá và A/B | `ml/`, `infra/k8s/agents/` | [Eval](llm-platform-agent-eval.md), [A/B](llm-platform-ab-test.md) |
| Vận hành dùng chung | `infra/k8s/observability/`, `ci/jenkins/` | [Quan sát](ml-llm-platform-observability.md), [Vault](ml-llm-platform-vault.md), [CI/CD](ml-llm-platform-cicd.md) |

[Kiến trúc ML + LLM Platform](diagrams/ml-llm-platform-system-architecture.html) · [Evidence ML Platform / LLM Platform](ml-llm-platform-evidence.md).

Agent truy vấn số liệu từ [Data Platform](data-platform.md), feature và dự đoán từ [ML Platform](ml-platform.md). API và hạ tầng vận hành dùng chung được dẫn chiếu từ cả hai platform.

[Mục lục tài liệu](README.md) · [Data & AI Platform](../README.md)
