<!-- Root EKS cho ML/LLM: plan/apply do người vận hành chạy, độc lập root Data Platform. -->
# Kubernetes AWS — ML Platform / LLM Platform

Root này tạo EKS, managed node group, quyền admin qua Access Entry, EKS Pod Identity Agent, EBS CSI và Pod Identity đọc feature store cho ServiceAccount `feature-store/feature-api`. Chưa triển khai Kubeflow, KServe, Redis, pgvector, LLM, ingress hoặc agent.

Đọc state local của `../terraform` sau khi root đó được apply với `enable_kubernetes_network=true` và hai AZ. Không sao chép state lên Git. `kubernetes_version`, `admin_role_arn`, CIDR quản trị và loại node là đầu vào bắt buộc; người vận hành phải kiểm quota, phiên bản hỗ trợ và ngân sách trước plan.

```bash
cp terraform.tfvars.example terraform.tfvars
# Điền các giá trị thật; state path tương đối với thư mục root này.
terraform init
terraform fmt -check
terraform validate
terraform plan -out kubernetes.tfplan
# Review plan trước khi apply; lệnh apply tạo dịch vụ AWS có chi phí.
terraform apply kubernetes.tfplan
terraform output -raw kubeconfig_command
```

- API private cho node, public chỉ nhận IP quản trị cụ thể. Profile dùng kubectl phải có identity tương ứng `admin_role_arn`. Node public subnet phục vụ egress, không cấp key SSH hay mở ứng dụng ra Internet.
- Node group dùng On-Demand x86_64, root disk mã hóa. Nhóm một node có thể gián đoạn khi cập nhật; sizing và số node phải chốt theo workload.
- EBS CSI dùng IAM policy `AmazonEBSCSIDriverPolicyV2`. Pod Identity được ràng buộc cluster, namespace và ServiceAccount qua request tags.
- Feature API chỉ có quyền đọc prefix `feature-store/` của bucket lakehouse. Role này chưa cấp quyền ghi training/MLflow hoặc publish registry Feast.
- Chỉ PostgreSQL/Lakekeeper được mở từ SG cluster vào SG EC2 theo mặc định. Listener Compose loopback cần cấu hình private riêng trước tích hợp; không tự mở các cổng host.
- State EKS phải được xử lý trước khi destroy root EC2/VPC, do còn subnet, security group, volume và dependency. Bảo toàn dữ liệu/PV và kiểm tài nguyên còn lại trước khi dọn.

Quy trình đầy đủ và gate: [cloud setup](../../../docs/shared/cloud_setup.md).

## Preset với quota cố định 16 vCPU

`demo.tfvars.example` (preset đã chốt) đặt hai worker c6i.xlarge để có dự phòng mức máy; `demo-peak.tfvars.example` đặt một worker m6i.2xlarge. Cả hai cho phép desired/min về 0 khi nghỉ, update strategy MINIMAL (đã dùng hết quota 16 vCPU) và worker tại subnet đầu để giữ AZ của EBS. Workload có thể gián đoạn khi update hoặc scale-to-zero; Kubernetes control plane vẫn chạy và tính phí. Không tự stop EC2 của managed node group. Kiểm tra quota vCPU và ngân sách trước khi áp preset.
