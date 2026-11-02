<!-- Mục đích: điểm vào Terraform AWS Data Platform: dựng gì, chạy thế nào, tốn gì. -->
# Terraform AWS Data Platform

Terraform ≥1.6, provider `hashicorp/aws` `~> 6.0` (bản cụ thể ghim trong
`.terraform.lock.hcl`). State để local. Mốc 1 **chưa có EKS**.

Dựng: VPC `10.42.0.0/16` + 1 public subnet + Internet Gateway, security group
**không mở cổng vào**, một EC2 `t3.xlarge` Ubuntu 24.04, một EBS gp3 dữ liệu,
IAM role (instance profile, không access key), 2 bucket S3 (`raw`, `lakehouse`),
budget tùy chọn. Sơ đồ: [`docs/diagrams/data-platform-system-architecture-excalidraw.png`](../../../docs/diagrams/data-platform-system-architecture-excalidraw.png).

Trước khi chạy, kiểm quyền của AWS profile, quota và cấu hình instance/region của tài khoản triển khai theo [runbook](../../../docs/aws-deploy.md). Repo chưa có bằng chứng launch EC2 trên AWS.

## Chạy

```bash
cd infra/aws/terraform
cp terraform.tfvars.example terraform.tfvars   # điền ssh_public_key
terraform init
terraform plan -out data-platform.tfplan
terraform apply data-platform.tfplan
```

Máy bạn cần `aws` CLI đã đăng nhập và `session-manager-plugin` (để SSH qua SSM).
Sau đó sang [Ansible](../ansible/README.md).

## Lưu ý tiền

- EC2 chỉ tính tiền khi chạy: `aws ec2 stop-instances --instance-ids <id>` khi nghỉ.
- **EBS vẫn tính tiền khi EC2 tắt.** Public IPv4 tự cấp của cấu hình này được
  AWS thu hồi khi stop; không có Elastic IP. Xem [quy tắc IP của EC2](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html).
- `cpu_credits = "standard"`: hết CPU credit thì máy chậm lại, không bị tính thêm.
- Budget chỉ cảnh báo, không chặn chi. Bật bằng `enable_budget = true` + `budget_email`.
- `terraform destroy` xóa cả EBS dữ liệu. Bucket còn dữ liệu sẽ chặn destroy
  trừ khi `force_destroy = true`.

## Khác bản GCP

| GCP (`infra/gcp`) | AWS (ở đây) |
|---|---|
| IAP + OS Login, firewall mở 22 cho dải IAP | SSM, security group không ingress |
| Service account gắn VM | IAM role + instance profile |
| `/dev/disk/by-id/google-platform-data` | `/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol…` |
| Budget tự gửi mail Billing Admin | Phải ghi `budget_email` |
| Bật API (`google_project_service`) | Không cần |

## State của môi trường đã tồn tại

Resource address hiện hành dùng `data_platform` và `operator`; tên AWS dùng tiền tố `platform` / `data-platform`. Nếu đã apply cấu hình có resource address hoặc tên cũ, cần kiểm state và lập kế hoạch di chuyển trước khi apply. Xem toàn bộ plan để phân biệt cập nhật metadata với thao tác tạo lại EC2, EBS hoặc bucket; đổi tên trong repo không tự di chuyển state hay dữ liệu.

## Registry và mạng cho ML/LLM

Bật `enable_ecr` để tạo repository ECR; bật `enable_kubernetes_network` và điền `secondary_availability_zone` để có hai subnet cho root [EKS](../kubernetes/README.md). Hai flag mặc định tắt. Xem [cloud setup](../../../docs/cloud-setup.md) để kiểm tài khoản, review plan và triển khai theo thứ tự.
