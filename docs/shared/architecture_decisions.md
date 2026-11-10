# Kiến trúc Data & AI Platform Data Platform đã triển khai

Nền tảng gom dữ liệu năm Channel của một Seller theo ba tầng Bronze, Silver,
Gold. [Sơ đồ triển khai](../diagrams/data-platform-system-architecture-excalidraw.png) mô tả container và luồng
dữ liệu; [thuật ngữ](terminology.md) định nghĩa các tầng và đối tượng nghiệp vụ.

## Iceberg, MinIO và REST catalog

Các bảng lakehouse dùng Iceberg, dữ liệu lưu trên MinIO; Lakekeeper cung cấp
REST catalog và lưu metadata vận hành trong PostgreSQL. Spark xử lý Batch,
Trino đọc các bảng qua catalog. Cấu hình và phép đo đã chạy được ghi trong
[Spark](../data-platform/reference/spark.md), [Storage](../data-platform/data_storage.md) và [Trino](../data-platform/reference/trino.md).

Bronze giữ payload JSON nguồn và provenance. Việc hai schema Amazon khác tên
và cấu trúc được xử lý rõ ràng trong Silver bằng hai schema parse, chuẩn hóa
cột rồi dedup; khả năng schema evolution của table format không tự thực hiện
phép ánh xạ payload này. Amazon SCD2 lấy các phiên bản đã quan sát trong Bronze,
không khôi phục những trạng thái chưa từng được xuất.

## Warehouse là bản sao phục vụ truy vấn

Gold Iceberg là đầu ra chính. Job xuất một lát Gold Walmart sang PostgreSQL
warehouse để đo index và so dữ liệu giữa hai hệ thống bằng Trino. Bằng chứng
hiện có là 1.875.607 Order Line Walmart; không tuyên bố đã nhân bản toàn bộ
Gold năm Channel. DDL index, EXPLAIN và phép đối chiếu cùng SKU nằm trong
[Storage](../data-platform/data_storage.md) và [Các cơ chế mở rộng](../data-platform/reference/data_integration.md).

## Điều phối và quản trị

Airflow điều phối DP1 nguồn → Bronze, DP2 Bronze → Silver/Gold/SCD2 và DP3
feature offline, với Validate sau mỗi chặng. Connection/Variable được lưu
trong Airflow để dùng chung giữa các DAG. Các job hiện ghi bảng rồi Validate;
không tuyên bố đã triển khai cơ chế branch Write-Audit-Publish.

DataHub nhận lineage và các assertion trích từ log Validate thành công. Các
assertion hiện là bằng chứng của run được ghi nhận; việc publish sau mỗi run
chưa được tự động hóa. Xem [Airflow](../data-platform/pipeline_orchestration.md) và [DataHub](../data-platform/data_governance.md).

## Phạm vi dữ liệu và tài nguyên

Manifest Batch ghi 100.012.790.412 byte cho Amazon, Walmart và SKU. Các run
DP1/DP2/DP3 dùng Amazon 10 ngày, bảng Walmart hiện có và bộ nguồn nhỏ của ba
Channel khác. Flink có proof baseline, late arrival, dedup và window trên
nguồn Stream được ghi trong [Flink](../data-platform/reference/flink.md).

Các service được bật theo từng nhóm để vừa RAM và đĩa máy phát triển. Không
khởi động Compose toàn bộ khi chỉ cần chạy lại một proof; các lệnh nêu đúng
service nằm trong tài liệu từng thành phần. Trạng thái và bằng chứng được
tổng hợp tại [trạng thái Data Platform](../data-platform/reference/data_platform_order_lifecycle_status.md) và [evidence](../data-platform/evidence.md).
