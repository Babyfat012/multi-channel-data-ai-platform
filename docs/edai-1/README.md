# EDAI-1 — Data Platform trên AWS

Tài liệu bám theo rubric, tập trung các lần triển khai AWS. Data Platform chạy Compose EC2/S3; ML/LLM chạy EKS `platform-apps`, MLflow/Jenkins/Vault trên EC2. Kết quả ghi rõ môi trường và giới hạn, không xác nhận dịch vụ đang bật.

![Kiến trúc triển khai AWS](../diagrams/data-platform-system-architecture-excalidraw.png)

## Đối chiếu rubric

| Dòng rubric | Tài liệu / bằng chứng | Kết quả và giới hạn |
|---|---|---|
| 3–4 | [Docker / Compose](docker.md) | Đo image EC2: 2,4 GB → 698 MB, cùng 27 package. |
| 5–10 | [Data Generator](data_generator.md) | Có volume, profile batch và manifest stream AWS. |
| 11–19 | [Processing Jobs](processing_jobs.md) | Có luồng stream và late arrival thật; bốn job Flink chạy liên tục RUNNING, Spark UI AWS thấy Union/Window dedup của Silver. |
| 20–21 | [Data Storage](data_storage.md) | Có số đo partition rewrite và EXPLAIN; DP1 có thể ghi lại spec ngày. |
| 22–27 | [Pipeline Orchestration](pipeline_orchestration.md) | Có graph, log ingest/validate và run ID AWS. |
| 28–33 | [Data Governance](data_governance.md) | Có lineage/assertion AWS; lineage được publish từ metadata pipeline. |
| 34–38 | [Schema Design](schema_design.md) | Có zone, DESCRIBE và ERD dim–fact: dbdiagram và ảnh DBeaver có đường nối trên Trino AWS. |
| 39–40 | [Novel Ideas](novel_ideas.md) | Có federation và probe CDC c/u/d; chưa đo CDC dưới tải. |

Mỗi trang có code/config, kết quả và ảnh có giải thích. Trạng thái và phần thiếu proof AWS: [kiểm kê rubric](../shared/rubric_cloud_audit.md).

## Triển khai và kiểm chứng

[Cloud setup](../shared/cloud_setup.md) · [AWS runbook](../shared/aws_deploy.md) · [IaC](../edai-2/iac.md).

[Ma trận bằng chứng AWS](evidence.md).

[Tham khảo kỹ thuật](reference/README.md) · [Mục lục chung](../README.md).
