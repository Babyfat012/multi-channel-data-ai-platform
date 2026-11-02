# Tài liệu Data & AI Platform

Điểm bắt đầu: [README dự án](../README.md). Chọn platform cần chạy hoặc tìm hiểu; mỗi trang thành phần dẫn tới code, cấu hình và bằng chứng liên quan.

Các tài liệu v1 ghi pipeline DP1/DP2/DP3 và phép đo đã chạy. Tài liệu `order-lifecycle` ghi pipeline v2, có phạm vi nguồn và nghiệm thu riêng; hai nhóm vẫn được dùng, không lấy bằng chứng của nhóm này để kết luận cho nhóm kia. Các con số trong báo cáo có giá trị tại ngày/lần chạy được ghi, không xác nhận service đang hoạt động.

## Data Platform

Bắt đầu: [phạm vi và code](data-platform.md) · [system architecture](diagrams/data-platform-system-architecture.html) · [trạng thái](data-platform-order-lifecycle-status.md).

| Chủ đề | Tài liệu |
|---|---|
| Business domain | [Domain](Domain.md), [thuật ngữ](terminology.md), [nguồn tham chiếu từng Channel](channel-sources.md) |
| Generator | [Thiết kế](generator-design.md), chất lượng [batch](data-quality-report-batch.md) / [stream](data-quality-report-stream.md) |
| Nguồn order lifecycle | [Contract](source-contract-order-lifecycle.md), [fixture](source-order-lifecycle-fixtures.md), [scale](source-order-lifecycle-scale.md) |
| Spark | [Pipeline và phép đo v1](spark.md), [order lifecycle v2](spark-order-lifecycle.md) |
| Silver | [Xử lý v1](silver.md), [năm Channel](silver-five-channels.md), [order lifecycle v2](silver-order-lifecycle.md), [hiệu năng v2](silver-order-lifecycle-performance.md) |
| Gold | [Kết quả v1](gold.md), [thiết kế](gold-design.md), [order lifecycle v2](gold-order-lifecycle.md), [contract nhãn](return-label-contract.md) |
| Schema | [Các zone](schema.md), [sơ đồ zone](diagrams/schema-zones.html), [ERD order lifecycle](diagrams/schema-order-lifecycle.md) |
| Storage và truy vấn | [Storage](storage.md), [Trino](trino.md), [warehouse order lifecycle](warehouse-order-lifecycle.md) |
| Streaming và CDC | [Flink v1](flink.md), [stream order lifecycle](stream-order-lifecycle.md), [CDC v1](cdc.md), [CDC order lifecycle](cdc-order-lifecycle.md) |
| Điều phối và governance | [Airflow v1](airflow.md), [Airflow order lifecycle](airflow-order-lifecycle.md), [DataHub v1](datahub.md), [DataHub order lifecycle](datahub-order-lifecycle.md) |
| Kiểm chứng đầu-cuối | [E2E order lifecycle local](e2e-order-lifecycle-local.md), [evidence](data-platform-evidence.md), [kiểm kê rubric × cloud](rubric-cloud-audit.md) |
| Các cơ chế mở rộng | [Tổng quan](data-integration.md), [data contract](data-contracts.md), [Iceberg maintenance](iceberg-maintenance.md) |

## ML Platform

Bắt đầu: [phạm vi và code](ml-platform.md) · [kiến trúc tổng thể](diagrams/ml-llm-platform-system-architecture.html).

| Chủ đề | Tài liệu |
|---|---|
| Feature store | [Feast offline / online](ml-platform-feature-store.md), [stream feature](ml-platform-stream-feature.md) |
| Vòng đời mô hình | [Training](ml-platform-training.md), [serving](ml-platform-serving.md), [drift](ml-platform-drift.md) |
| API công cụ | [Feature API, REST và MCP](ml-llm-platform-web-api.md) |
| Kiểm chứng | [Evidence ML Platform / LLM Platform](ml-llm-platform-evidence.md) |

## LLM Platform

Bắt đầu: [phạm vi và code](llm-platform.md) · [kiến trúc tổng thể](diagrams/ml-llm-platform-system-architecture.html).

| Chủ đề | Tài liệu |
|---|---|
| Agent và inference | [LLM, agent và điều phối](llm-platform-agents.md) |
| Truy xuất tài liệu | [RAG](llm-platform-rag.md) |
| Truy cập | [Gateway](llm-platform-gateway.md), [API công cụ](ml-llm-platform-web-api.md) |
| Đánh giá | [Agent eval](llm-platform-agent-eval.md), [A/B](llm-platform-ab-test.md) |
| Kiểm chứng | [Evidence ML Platform / LLM Platform](ml-llm-platform-evidence.md) |

Hạ tầng vận hành dùng chung giữa các platform: [CI/CD](ml-llm-platform-cicd.md), [observability](ml-llm-platform-observability.md), [Vault](ml-llm-platform-vault.md).

## Triển khai và phát triển

[Cấu hình demo và ngân sách](cloud-capacity.md): lựa chọn tài nguyên, chi phí và lịch bật/tắt.

[Cloud setup](cloud-setup.md) là điểm bắt đầu cho triển khai AWS cả ba platform: preflight tài khoản, EC2/S3/ECR, EKS, storage và các gate kiểm runtime.

- [Docker Compose](Docker.md): môi trường nguồn, profile và image.
- [AWS runbook](aws-deploy.md), [IaC](IaC.md): đích EC2 + S3, thao tác triển khai và dọn tài nguyên.
- Sơ đồ AWS: [hạ tầng](diagrams/aws-infra.html), [Data/ML Platform](diagrams/aws-architecture-layer12.html), [LLM Platform và Agent](diagrams/aws-architecture-layer3.html).
- [Kiến trúc đã triển khai](architecture-decisions.md), [ADR](adr/README.md), [code style](code-style.md), [GLOSSARY](../GLOSSARY.md).

## Hợp đồng thiết kế và nghiệm thu

[Publication contract](run-publication-contract.md) mô tả thiết kế công bố dữ liệu; các gate và POC còn mở được ghi ngay trong tài liệu. Thiết kế và ADR ghi quyết định, không tự chứng minh tính năng đã triển khai. Đích AWS và trạng thái runtime xem runbook, trạng thái và ma trận bằng chứng phía trên.

## Bằng chứng và tài liệu lịch sử

[Đối chiếu tài liệu theo rubric](rubric-guide.md) dành cho coursework; hướng dẫn sử dụng platform nằm ở các mục trên.

`evidence/` giữ receipt, log và metric; `images/` giữ ảnh chụp. Tìm bằng chứng qua tài liệu thành phần hoặc evidence để biết ngữ cảnh và giới hạn của từng run.

Artifact lịch sử giữ tên và nội dung gốc để có thể truy vết. Báo cáo evidence nêu rõ môi trường, ngày chạy và các phần chưa kiểm chứng.

## Cập nhật tài liệu

- Sửa trang thành phần đang có khi thay đổi cách chạy hoặc cấu hình; thêm trang mới khi có chủ đề riêng và dẫn nó từ mục lục này hoặc trang platform.
- Ghi ngày, phạm vi và giới hạn khi đưa số đo vào tài liệu; liên kết đến receipt hoặc ảnh gốc.
- Giữ evidence gốc bất biến; cập nhật bản tổng hợp khi có run mới, không sửa số đo lịch sử.
- Kế hoạch cá nhân và ghi chú bàn giao nằm ngoài tài liệu công khai. Kiểm tra link và khả năng tìm tài liệu bằng `uv run pytest -q tests/unit/test_docstrings.py`.
