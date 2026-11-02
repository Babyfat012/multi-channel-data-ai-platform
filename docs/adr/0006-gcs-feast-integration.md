---
status: superseded
---

# GCS và Feast Spark/Redis cho thử nghiệm Cloud đầu tiên

> **Lịch sử.** Phần GCS đã được thay bằng AWS S3 (2026-10-22); Feast + Redis vẫn dùng. Hiện hành: [AWS runbook](../shared/aws_deploy.md), [Feature store](../edai-2/feature_store.md).

Q26 đã xác nhận chuyển online store từ đề xuất PostgreSQL sang Redis.
PostgreSQL tiếp tục phục vụ warehouse; GCS và tiêu chí POC vẫn được giữ.

Người dùng chốt Q22–Q24: GCS giữ raw và Iceberg, tạo warehouse Cloud mới và
chạy lại pipeline từ nguồn. Chấp nhận sửa các cấu hình S3/MinIO để dữ liệu
bền vững độc lập với compute. Proof local giữ làm tham chiếu; không coi copy
file sang GCS là migration metadata Iceberg hợp lệ.

Feast dùng Spark offline đọc Iceberg và Redis online cho POC trên GCE,
không mặc định dùng managed Redis. Lựa chọn theo trải nghiệm học và hướng
ML serving người dùng mong muốn, chấp nhận thêm service và quản lý RAM/
khôi phục thay vì tận dụng PostgreSQL online. Adapter cần pin/test; vẫn
kiểm tra độ mới tường minh và phục hồi/rematerialize khi bật hệ thống lại.

POC phải chứng minh đúng khóa/thời gian, missing/freshness, parity offline/
online, rerun, giữ bản tốt khi publish lỗi, stop/start và số liệu UI. Đây là
lựa chọn để kiểm chứng, không phải bằng chứng tích hợp đã chạy thành công.

Mọi thao tác Cloud do người dùng tự làm tay. Trợ lý chuẩn bị cấu hình và
hướng dẫn có giải thích, không tự thực hiện thao tác Cloud. PostgreSQL và
catalog cần lưu trữ bền vững/backup riêng; còn xác định region, sizing,
retention và dự toán trước full-scale.
