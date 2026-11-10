# Data Platform

Data Platform chuẩn hóa dữ liệu bán hàng đa kênh, cung cấp dữ liệu có chất lượng và truy vết cho phân tích, ML Platform và LLM Platform.

| Phạm vi | Code / cấu hình | Tài liệu |
|---|---|---|
| Nguồn giả lập và hợp đồng dữ liệu | `generator/`, `configs/`, `contracts/` | [Generator](generator_design.md), [source contract](source_contract_order_lifecycle.md) |
| Batch, lakehouse và warehouse | `spark_jobs/`, `sql/`, `trino/` | [Spark](spark.md), [Silver](silver.md), [Gold](gold.md), [Storage](../data_storage.md) |
| Streaming và CDC | `flink_jobs/` | [Flink](flink.md), [CDC](cdc.md) |
| Điều phối và quản trị | `dags/` | [Airflow](../pipeline_orchestration.md), [DataHub](../data_governance.md) |
| Triển khai | `docker-compose.yml`, `infra/aws/` | [Docker](../docker.md), [AWS](../../shared/aws_deploy.md) |

[Kiến trúc Data Platform và ML Platform](../../diagrams/data-platform-system-architecture-excalidraw.png) · [Evidence](../evidence.md) · [Tiến độ](data_platform_order_lifecycle_status.md).

Dữ liệu Gold và các bảng feature offline là đầu vào cho [ML Platform](../../ml-llm-platform/reference/ml_platform.md). [LLM Platform](../../ml-llm-platform/reference/llm_platform.md) sử dụng dữ liệu qua API công cụ và nguồn tài liệu đã kiểm tra.

[Mục lục tài liệu](../../README.md) · [Data & AI Platform](../../../README.md)
