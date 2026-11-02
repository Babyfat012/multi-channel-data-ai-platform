# Iceberg với REST catalog và warehouse PostgreSQL

## Quyết định

Bronze, Silver và Gold dùng Apache Iceberg trên object storage. Lakekeeper cung cấp REST catalog. PostgreSQL warehouse giữ bản sao phục vụ truy vấn của một lát Gold; lakehouse là nguồn đầu ra chính.

## Lý do

- Table format cung cấp snapshot và metadata cho việc đọc/ghi bảng, thay vì quản lý trực tiếp các file Parquet rời.
- REST catalog tạo điểm cấu hình chung cho Spark, Flink và Trino. Runtime jar Iceberg phải khớp phiên bản engine.
- PostgreSQL phục vụ lookup và thử nghiệm index, trong khi Iceberg phục vụ quét dữ liệu. Hai backend có mục đích khác nhau và cần đối soát cùng input.

Các lựa chọn đã cân nhắc gồm Delta Lake, Parquet rời, JDBC catalog và Hadoop catalog. Quyết định trong repo là Iceberg + Lakekeeper; các benchmark được dẫn phía dưới chỉ kiểm chứng cấu hình đã chạy, không so sánh đầy đủ mọi lựa chọn.

## Hệ quả và giới hạn

- Gold → warehouse có một chiều đồng bộ; không sửa warehouse để thay đổi Gold.
- Các writer phải xử lý commit conflict và retry phù hợp.
- Có thêm service catalog và metadata database; bật Compose theo profile để kiểm soát tài nguyên.
- Schema evolution của table format không tự chuẩn hóa payload nguồn. Amazon Orders v0 và 2026 cần mapping tại Silver.
- Write-Audit-Publish là một hướng thiết kế; pipeline v1 hiện ghi bảng rồi validate, chưa có bằng chứng triển khai WAP toàn chuỗi.

## Evidence

[Storage](../edai-1/data_storage.md) ghi compaction và EXPLAIN trước/sau index. [Trino](../edai-1/reference/trino.md) ghi phép đối chiếu cùng SKU giữa Gold và warehouse. [Kiến trúc hiện hành](../shared/architecture_decisions.md) nêu phạm vi bảng đã chạy; [Data Platform evidence](../edai-1/evidence.md) tổng hợp receipt và giới hạn local/cloud.
