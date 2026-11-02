# Data Platform

Data Platform chuẩn hóa dữ liệu bán hàng đa kênh, cung cấp dữ liệu có chất lượng và truy vết cho phân tích, ML Platform và LLM Platform.

| Phạm vi | Code / cấu hình | Tài liệu |
|---|---|---|
| Nguồn giả lập và hợp đồng dữ liệu | `generator/`, `configs/`, `contracts/` | [Generator](generator-design.md), [source contract](source-contract-order-lifecycle.md) |
| Batch, lakehouse và warehouse | `spark_jobs/`, `sql/`, `trino/` | [Spark](spark.md), [Silver](silver.md), [Gold](gold.md), [Storage](storage.md) |
| Streaming và CDC | `flink_jobs/` | [Flink](flink.md), [CDC](cdc.md) |
| Điều phối và quản trị | `dags/` | [Airflow](airflow.md), [DataHub](datahub.md) |
| Triển khai | `docker-compose.yml`, `infra/aws/` | [Docker](Docker.md), [AWS](aws-deploy.md) |

[Kiến trúc Data Platform và ML Platform](diagrams/data-platform-system-architecture-excalidraw.png) · [Evidence](data-platform-evidence.md) · [Tiến độ](data-platform-order-lifecycle-status.md).

Dữ liệu Gold và các bảng feature offline là đầu vào cho [ML Platform](ml-platform.md). [LLM Platform](llm-platform.md) sử dụng dữ liệu qua API công cụ và nguồn tài liệu đã kiểm tra.

[Mục lục tài liệu](README.md) · [Data & AI Platform](../README.md)
