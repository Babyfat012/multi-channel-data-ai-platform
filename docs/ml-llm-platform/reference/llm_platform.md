# LLM Platform

LLM Platform cung cấp hội thoại cho Seller bằng LLM, RAG và agent sử dụng dữ liệu, mô hình của hệ thống.

| Phạm vi | Code / cấu hình | Tài liệu |
|---|---|---|
| LLM inference, agent và điều phối | `infra/k8s/agents/` | [Agent](llm_platform_agents.md) |
| Tài liệu, embedding và truy xuất | `rag/`, `dags/ml_rag_ingest.py` | [RAG](../rag_pipeline.md) |
| Gateway và truy cập công cụ | `infra/k8s/agents/`, `serving/` | [Gateway](../routing_gateway.md), [API](../web_api_mcp.md) |
| Đánh giá và A/B | `ml/`, `infra/k8s/agents/` | [Eval](../agent_evaluation.md), [A/B](../ab_testing.md) |
| Vận hành dùng chung | `infra/k8s/observability/`, `ci/jenkins/` | [Quan sát](../observability.md), [Vault](../security.md), [CI/CD](../cicd.md) |

[Kiến trúc ML + LLM Platform](../../diagrams/ml-llm-platform-system-architecture-excalidraw.png) · [Evidence ML Platform / LLM Platform](ml_llm_platform_evidence.md).

Agent truy vấn số liệu từ [Data Platform](../../data-platform/reference/data_platform.md), feature và dự đoán từ [ML Platform](ml_platform.md). API và hạ tầng vận hành dùng chung được dẫn chiếu từ cả hai platform.

[Mục lục tài liệu](../../README.md) · [Data & AI Platform](../../../README.md)
