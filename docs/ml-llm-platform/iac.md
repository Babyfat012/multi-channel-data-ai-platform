# Infrastructure as Code trên AWS

EDAI-2, dòng 44–45: dựng hạ tầng AWS bằng code (Terraform) và cấu hình máy bằng code (Ansible).

**Kết luận:** `terraform plan` trên hạ tầng thật báo **No changes**, nghĩa là hạ tầng đang chạy khớp hoàn toàn với code.
Playbook Ansible chạy trên EC2 kết thúc với `failed=0`.

Các kết quả dưới đây là của những lần chạy trên AWS; tài liệu không khẳng định dịch vụ hiện vẫn đang bật.

## Phạm vi triển khai

Hạ tầng được dựng ở region Sydney (`ap-southeast-2`), gồm: máy EC2 chạy Data Platform, hai bucket S3 (raw và lakehouse), ECR và cụm EKS `platform-apps`.
EC2 và hai worker của EKS dùng loại máy `r6i.xlarge`. Số liệu của các lần chạy cũ vẫn ghi đúng loại máy tại thời điểm đo.

| Thành phần | Code và nhiệm vụ |
|---|---|
| Terraform Data Platform | [Root module](../../infra/aws/terraform/README.md): VPC, EC2/EBS, IAM, S3, ECR và mạng dùng cho EKS |
| Terraform EKS | [Root module](../../infra/aws/kubernetes/README.md): cluster, node, identity và quyền workload |
| Ansible | [Playbook/roles](../../infra/aws/ansible/README.md): base, data disk, app, DataHub, source data và smoke trên EC2 |

Ứng dụng trên EC2 lấy quyền AWS qua IAM role; ứng dụng trên EKS lấy quyền qua Pod Identity. Không có secret nào nằm trong repository. Trình tự chạy: [Cloud setup](../shared/cloud_setup.md), [AWS runbook](../shared/aws_deploy.md).

## Terraform đã chạy

Chạy `terraform plan` trên hạ tầng đang chạy. Terraform đọc lại trạng thái thật của từng tài nguyên (VPC, subnet, security group, EC2, EBS,
S3 raw/lakehouse, IAM role và policy `platform-ci-ecr`, ECR repo, budget) rồi so với code.

Kết quả là **No changes**: không có tài nguyên nào lệch so với code. Điều này vẫn đúng sau một sự cố trước đó:
EC2 từng bị Terraform thay mới khi máy được stop, lỗi này đã sửa bằng `lifecycle.ignore_changes`.

![terraform plan: refresh từng tài nguyên AWS, No changes](../evidence/aws/terraform-plan-no-changes-20261031.png)

## Ansible đã chạy

Chạy toàn bộ playbook `site.yml` lên EC2 qua SSM (Session Manager).
Lệnh truyền IP hiện tại của node EKS cho Redis và pgvector bằng `-e feast_redis=… -e rag_pg=…`. Nếu thiếu hai tham số này, file `.env` sẽ quay về IP mặc định cũ.

Ảnh dưới cho thấy các task của role `app` (kiểm checksum Dockerfile, tạo khoá Fernet, compose up, MLflow, Lakekeeper, Variable Airflow) và role `datahub`.
Kết quả cuối (PLAY RECAP) có 4 task `changed`; trong ảnh thấy được 2 trong số đó (bật dịch vụ Data Platform, bật MLflow).
Phần lớn task là `ok` vì đây là lần chạy lại, không có gì cần thay đổi.

![Ansible site.yml: các task role app/datahub, PLAY RECAP ok=51 changed=4 failed=0](../evidence/aws/ansible-play-recap-20261031.png)

Bằng chứng triển khai là ảnh `terraform plan` (so code với hạ tầng thật), không phải `terraform validate` (chỉ kiểm cú pháp code).
Cách chạy pipeline và chụp ảnh cho từng dòng Data Platform nằm ở [bảng evidence](../shared/aws_deploy.md#anh).

Vận hành:

- Khi không dùng, [dừng EC2](../shared/aws_deploy.md#dung-don). EBS và S3 vẫn bị tính tiền khi máy dừng.
- Khi muốn huỷ hẳn môi trường: sao lưu trước, rồi chạy `terraform destroy`, sau đó vào Console kiểm các version còn lại trong bucket và tài nguyên còn sót.

<details>
<summary>Tham khảo: thiết kế, hướng dẫn phát triển và ghi nhận trước triển khai AWS</summary>

Các ghi nhận local giữ nguyên môi trường, không dùng thay bằng chứng cloud.

<!-- Mục đích: mô tả Terraform/Ansible AWS, quyết định bảo mật và vị trí proof IaC cho phép đo. -->
## Terraform tạo gì và vì sao

| Tài nguyên | Cấu hình / quyết định bảo mật |
|---|---|
| VPC, public subnet, Internet Gateway, route và association | VPC riêng `10.42.0.0/16`, subnet `10.42.0.0/24`. Chiều ra (egress) dùng để kéo image, gọi SSM và S3. Cấu hình hiện hành dùng public IPv4 thay cho NAT Gateway |
| Security group | **Không cho kết nối vào (ingress)**, chỉ cho kết nối ra IPv4; không mở SSH hay UI ra Internet. SSM agent trên máy tự gọi ra AWS, nên SSH đi qua Session Manager |
| EC2 + key pair | Ubuntu 24.04 Canonical, `t3.xlarge` 4 vCPU/16 GiB; chỉ đưa public key vào Terraform. Key private giữ trên laptop; CPU credit `standard` tránh phí surplus |
| Metadata EC2 | Bắt buộc **IMDSv2**. Hop limit đặt **2** để container (nằm sau thêm một tầng mạng) vẫn lấy được credential; không nhúng access key vào Compose |
| EBS root + EBS data/attachment | gp3 mặc định 30 + 100 GiB, **encrypted=true**. Disk data độc lập để thay EC2 vẫn giữ dữ liệu; `terraform destroy` vẫn xóa disk. EC2/EBS cùng AZ |
| IAM role, inline policy, instance profile | EC2 nhận credential tạm; quyền list/location trên hai bucket và đọc/ghi/xóa object trong chính hai bucket. Không có quyền S3 toàn tài khoản; gắn `AmazonSSMManagedInstanceCore` cho SSM |
| Hai S3 bucket `raw`, `lakehouse` | Tên kèm account ID tránh trùng toàn cầu; **block public** cả bốn tùy chọn, `BucketOwnerEnforced` tắt ACL, quyền qua IAM |
| S3 versioning + lifecycle | Giữ version; xóa noncurrent sau 30 ngày, `tmp/` sau 7 ngày và multipart chưa xong sau 7 ngày. Không đặt dữ liệu chính dưới `tmp/`; không có rule xóa current Iceberg toàn bucket |
| Budget (bật trong runbook) | Email khi vượt 50/90/100% ngân sách; không trừ credit khi tính cảnh báo. Không phải giới hạn cứng hay lịch dừng máy |

Để dùng SSM, profile AWS của người vận hành phải có quyền mở phiên, và agent SSM trên EC2 phải đang online.

Lưu ý:

- Không cho kết nối vào không có nghĩa là EC2 không ra được Internet: subnet có route qua Internet Gateway và máy có IPv4 public.
- Ansible và ứng dụng chỉ mở port UI ở loopback (`127.0.0.1`). Muốn xem UI thì mở tunnel SSH theo output của Terraform.
- IAM role có quyền xoá object vì Iceberg cần xoá file khi quản lý bảng. Versioning chỉ giữ bản cũ trong thời gian có hạn, nên không thay được sao lưu.

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

Kết quả mong đợi: Terraform tạo đúng các tài nguyên trong bảng; PLAY RECAP có `failed=0` và bước smoke thành công.
Các lỗi thường gặp (IAM, quota, SSM, key, secret) và cách xử lý: [Terraform](../shared/aws_deploy.md#terraform), [Ansible](../shared/aws_deploy.md#ansible).

State local, file plan, inventory, `.env`, secret và vault password không được đưa vào hồ sơ công khai.
Chỉ ảnh và receipt (file biên nhận kết quả) đã kiểm mới được đặt trong `docs/`.

</details>

[Mục lục phần này](README.md) · [Mục lục chung](../README.md)
