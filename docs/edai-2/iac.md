# Infrastructure as Code trên AWS

Các kết quả dưới đây thuộc các lần chạy AWS được ghi ngày, không xác nhận dịch vụ đang bật.

## Phạm vi triển khai

Hạ tầng đã ghi nhận tại Sydney (`ap-southeast-2`): EC2 Data Platform, S3 raw/lakehouse, ECR và EKS `platform-apps`. Ngày 31/10, EC2 và hai EKS worker dùng `r6i.xlarge`; các lần chạy cũ giữ đúng cấu hình máy tại thời điểm đo.

| Thành phần | Code và nhiệm vụ |
|---|---|
| Terraform Data Platform | [Root module](../../infra/aws/terraform/README.md): VPC, EC2/EBS, IAM, S3, ECR và mạng dùng cho EKS |
| Terraform EKS | [Root module](../../infra/aws/kubernetes/README.md): cluster, node, identity và quyền workload |
| Ansible | [Playbook/roles](../../infra/aws/ansible/README.md): base, data disk, app, DataHub, source data và smoke trên EC2 |

Workload EC2 dùng IAM role, workload EKS dùng Pod Identity; secret không đặt trong repository. Trình tự chạy: [Cloud setup](../shared/cloud_setup.md), [AWS runbook](../shared/aws_deploy.md).

## Terraform đã chạy (2026-10-31)

`terraform plan` trên hạ tầng đang chạy: Terraform đọc lại từng tài nguyên thật (VPC, subnet, SG, EC2, EBS, S3 raw/lakehouse,
IAM role + policy `platform-ci-ecr`, ECR repo, budget) và so với code ⇒ **No changes**: hạ tầng khớp code, kể cả sau
sự cố 30/10 (EC2 bị thay khi stop, đã sửa bằng `lifecycle.ignore_changes`).

![terraform plan: refresh từng tài nguyên AWS, No changes](../evidence/aws/terraform-plan-no-changes-20261031.png)

## Ansible đã chạy (2026-10-31)

Playbook đầy đủ `site.yml` trên EC2 qua SSM, truyền IP node EKS hiện tại cho Redis/pgvector (`-e feast_redis=…
-e rag_pg=…`; thiếu thì `.env` về IP mặc định cũ). Task theo role `app` (checksum Dockerfile, Fernet, compose up,
MLflow, Lakekeeper, Variable Airflow) và `datahub`; trong ảnh thấy 2 trong 4 `changed` (bật dịch vụ Data Platform, bật MLflow); phần lớn task `ok` vì chạy lại không đổi gì.

![Ansible site.yml: các task role app/datahub, PLAY RECAP ok=51 changed=4 failed=0](../evidence/aws/ansible-play-recap-20261031.png)

Ảnh plan xác nhận trạng thái đối chiếu hạ tầng; không dùng `terraform validate` thay bằng chứng triển khai. Quy trình thu pipeline/ảnh theo từng dòng Data Platform
ở [bảng evidence](../shared/aws_deploy.md#anh). Khi nghỉ
[dừng EC2](../shared/aws_deploy.md#dung-don); EBS/S3 còn tính tiền. Khi hủy môi trường sao lưu rồi
`terraform destroy`, kiểm bucket version và tài nguyên còn lại trong Console.

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên ngày và môi trường, không dùng thay bằng chứng cloud.

<!-- Mục đích: mô tả Terraform/Ansible AWS, quyết định bảo mật và vị trí proof IaC cho phép đo. -->
## Terraform tạo gì và vì sao

| Tài nguyên | Cấu hình / quyết định bảo mật |
|---|---|
| VPC, public subnet, Internet Gateway, route và association | VPC riêng `10.42.0.0/16`, subnet `10.42.0.0/24`; egress kéo image, gọi SSM và S3. Dùng public IPv4 thay NAT Gateway ở cấu hình hiện hành |
| Security group | **Không ingress**, egress IPv4; không mở SSH/UI ra Internet. SSM agent chủ động gọi ra AWS, SSH chạy qua Session Manager |
| EC2 + key pair | Ubuntu 24.04 Canonical, `t3.xlarge` 4 vCPU/16 GiB; chỉ đưa public key vào Terraform. Key private giữ trên laptop; CPU credit `standard` tránh phí surplus |
| Metadata EC2 | Bắt buộc **IMDSv2**, hop limit **2** cho container lấy credential qua thêm một hop; không nhúng access key vào Compose |
| EBS root + EBS data/attachment | gp3 mặc định 30 + 100 GiB, **encrypted=true**. Disk data độc lập để thay EC2 vẫn giữ dữ liệu; `terraform destroy` vẫn xóa disk. EC2/EBS cùng AZ |
| IAM role, inline policy, instance profile | EC2 nhận credential tạm; quyền list/location trên hai bucket và đọc/ghi/xóa object trong chính hai bucket. Không có quyền S3 toàn tài khoản; gắn `AmazonSSMManagedInstanceCore` cho SSM |
| Hai S3 bucket `raw`, `lakehouse` | Tên kèm account ID tránh trùng toàn cầu; **block public** cả bốn tùy chọn, `BucketOwnerEnforced` tắt ACL, quyền qua IAM |
| S3 versioning + lifecycle | Giữ version; xóa noncurrent sau 30 ngày, `tmp/` sau 7 ngày và multipart chưa xong sau 7 ngày. Không đặt dữ liệu chính dưới `tmp/`; không có rule xóa current Iceberg toàn bucket |
| Budget (bật trong runbook) | Email khi vượt 50/90/100% ngân sách; không trừ credit khi tính cảnh báo. Không phải giới hạn cứng hay lịch dừng máy |

SSM yêu cầu profile người vận hành có quyền mở phiên và EC2 agent online.
Không ingress không đồng nghĩa EC2 không ra Internet: subnet có route qua IGW
và máy có IPv4 public. Ansible/ứng dụng giữ port UI ở loopback và mở tunnel
SSH theo output Terraform. IAM role có quyền xóa object để quản lý Iceberg;
versioning có retention hữu hạn, không thay sao lưu.

## Cấu trúc và Ansible roles

```text
infra/aws/
├── terraform/
│   ├── versions.tf, variables.tf       # provider, region, quota cấu hình máy
│   ├── network.tf, compute.tf          # mạng, EC2, EBS, IMDSv2
│   ├── iam.tf, storage.tf, budget.tf   # role, S3, cảnh báo chi phí
│   ├── outputs.tf                     # ID/bucket/region và tunnel SSM
│   └── terraform.tfvars.example
└── ansible/
    ├── site.yml, requirements.yml
    ├── inventory.example.ini
    ├── group_vars/all.example.yml
    └── roles/                         # base, data_disk, app, datahub, source_data, smoke
```

| Role | Việc làm |
|---|---|
| `base` | Kiểm Ubuntu 24.04; cài Docker/Compose, AWS CLI v2, Python venv/rsync; thêm user vào nhóm docker |
| `data_disk` | Tìm EBS theo volume ID NVMe, kiểm/mount disk vào `/mnt/platform-data`; không dựa vào tên `/dev/sdf` trên Nitro |
| `app` | Rsync repo có loại secret/dữ liệu local; render `.env` mode 0600, tạo Fernet, build driver/executor cùng Python; bật PostgreSQL/Lakekeeper/Spark/Airflow, bootstrap warehouse S3 và Variable Airflow; venv `/mnt/platform-data/venv` |
| `datahub` (mặc định `enable_datahub: true`) | Chuẩn bị uv, CLI `acryl-datahub==1.7.0.14`, Compose quickstart và secret riêng mode 0600; **không khởi động DataHub**. CLI cài cho user SSH, pha live chạy sau khi dừng Spark/Airflow |
| `source_data` (tag riêng) | Sinh fixture hoặc scale theo tên dataset; sync S3 không `--delete`, loại `_truth/`; nạp Walmart vào PostgreSQL |
| `smoke` (tag riêng) | Kiểm runtime và danh tính container, đọc/ghi S3/Iceberg trước pipeline |

## Cách chạy và thu proof

Tại repo root, chuẩn bị profile AWS, công cụ và key theo
[chuẩn bị](../shared/aws_deploy.md#chuan-bi), sau đó:

```bash
cd infra/aws/terraform
cp terraform.tfvars.example terraform.tfvars
## Điền public key, region/AZ theo môi trường Sydney, enable_budget=true và email thật.
terraform init
terraform plan -out data-platform.tfplan
terraform apply data-platform.tfplan
terraform output
cd ../ansible
ansible-galaxy collection install -r requirements.yml
cp inventory.example.ini inventory.ini
cp group_vars/all.example.yml group_vars/all.yml
## Điền instance ID/region vào inventory; bucket/volume/repo/secret vào all.yml.
ansible-vault encrypt group_vars/all.yml
ansible-playbook -i inventory.ini site.yml --ask-vault-pass
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags source_data
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags smoke
```

Mong đợi: Terraform tạo đúng tài nguyên trong bảng; PLAY RECAP `failed=0` và
smoke thành công. Lỗi IAM/quota/SSM/key/secret và cách xử lý tại
[Terraform](../shared/aws_deploy.md#terraform), [Ansible](../shared/aws_deploy.md#ansible).
State local, plan, inventory, `.env`, secret và vault password giữ ngoài hồ
sơ công khai; chỉ ảnh/receipt đã kiểm mới đặt trong `docs/`.

</details>

[Mục lục phần này](../README.md) · [Mục lục chung](../../README.md)
