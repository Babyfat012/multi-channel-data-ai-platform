---
status: accepted
---

# Gợi ý cho Seller và phạm vi triển khai

Phiên bản đầu cung cấp gợi ý để Seller quyết định; hệ thống không tự đổi giá, hủy đơn, hoàn tiền hoặc gửi thông báo. Insight tập trung vào bán hàng, xu hướng theo Storefront, rủi ro trả hàng và gợi ý kiểm tra. Inventory, traffic và ads là các nhánh mở rộng.

Local phục vụ phát triển và kiểm thử. Triển khai cloud dùng IaC và được kích hoạt chủ động bởi người vận hành. Việc có manifest hoặc pipeline triển khai không đồng nghĩa đã có runtime cloud.

Lựa chọn GCP trước đây được ghi tại [ADR lịch sử](0003-gcp-on-demand.md); đích hiện hành là [AWS EC2 + S3](../shared/aws_deploy.md). ML Platform / LLM Platform trên EKS chưa có evidence cloud.
