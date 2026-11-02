---
status: superseded
---

# GCP on-demand — quyết định lịch sử

Ngày 2026-10-17, thiết kế ban đầu chọn GCP, GCE cho Data Platform/Feast và Kubernetes cho ML Platform/LLM Platform. Các cấu hình GCP hiện được lưu nội bộ; repo không có bằng chứng triển khai GCP.

Quyết định đích triển khai này đã được thay bằng AWS EC2 + S3 vào 2026-10-22. Cấu hình và hướng dẫn hiện hành: [AWS runbook](../shared/aws_deploy.md), [IaC](../edai-2/iac.md). Inventory overlay GCP trong evidence chỉ được thu local, không chứng minh runtime cloud.

Nguyên tắc cấu hình môi trường và tái tạo runtime tiếp tục được ghi tại [local/cloud](0004-local-development-cloud-delivery.md).
