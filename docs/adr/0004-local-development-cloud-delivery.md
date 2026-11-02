---
status: accepted
---

# Local để phát triển và kiểm thử, cloud để triển khai

Local dùng cho phát triển, kiểm thử và phép đo trong phạm vi tài nguyên hiện có. Cloud là môi trường triển khai riêng, cần cấu hình và evidence tương ứng; kết quả local không chứng minh runtime cloud.

Thiết kế cần cấu hình môi trường rõ ràng, lưu dữ liệu/metadata bền vững và khả năng tái tạo runtime. Kiểm tích hợp với input nhỏ trước khi mở rộng, và đo lại trên môi trường đích.

Các lựa chọn GCE/GCP ban đầu là lịch sử. Đích hiện hành là [AWS EC2 + S3](../shared/aws_deploy.md); ML Platform/LLM Platform trên EKS chưa có evidence triển khai.
