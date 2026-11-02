# Tài liệu Data & AI Platform

Tài liệu chia theo hai phần rubric, tập trung hệ thống đã triển khai trên AWS tại Sydney (`ap-southeast-2`). Các kết quả gắn với ngày/lần chạy, không xác nhận dịch vụ đang hoạt động.

| Phần | Phạm vi | Điểm bắt đầu |
|---|---|---|
| EDAI-1 | Generator, Spark/Flink, lakehouse/warehouse, Airflow, DataHub và schema | [EDAI-1](edai-1/README.md) |
| EDAI-2 | LLM/Agent, RAG, feature store, training/serving, CI/CD và vận hành | [EDAI-2](edai-2/README.md) |

Data Platform chạy Compose trên EC2, raw/lakehouse ở S3. ML/LLM chạy EKS `platform-apps`; MLflow, Jenkins và Vault chạy EC2.

## Domain và triển khai

[Business domain](shared/domain.md) · [Thuật ngữ](shared/terminology.md) · [Cloud setup](shared/cloud_setup.md) · [AWS runbook](shared/aws_deploy.md) · [IaC](edai-2/iac.md).

## Rubric và bằng chứng

[Đối chiếu rubric](shared/rubric_guide.md) · [Kiểm kê AWS và phần còn thiếu](shared/rubric_cloud_audit.md) · [Data Platform evidence](edai-1/evidence.md).

Ảnh nhúng trong trang thành phần và giải thích mục đích. `evidence/` giữ receipt/log/metric; `images/` giữ ảnh gốc. Giữ nguyên định danh và số liệu từng lần chạy. Hướng dẫn local và ghi nhận trước triển khai ở phần tham khảo, ghi môi trường riêng.

## Tài liệu dùng chung

[Mục lục dùng chung](shared/README.md) · [ADR](adr/README.md) · [Sơ đồ](diagrams/README.md) · [GLOSSARY](../GLOSSARY.md).

Khi cập nhật: sửa trang thành phần, ghi ngày/môi trường/giới hạn số đo, giữ evidence gốc và kiểm tra link bằng `uv run pytest -q tests/unit/test_docstrings.py`.

[README dự án](../README.md)
