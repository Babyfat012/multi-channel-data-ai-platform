<!-- Mục đích: runbook người vận hành triển khai Data Platform batch EC2 + S3, thu proof và dọn tài nguyên. -->
# Triển khai AWS — Data Platform

Môi trường đã triển khai: Sydney `ap-southeast-2`, EC2 Data Platform/S3 và EKS `platform-apps` cho ML/LLM. Các lần chạy được đối chiếu tại [Data Platform evidence](../edai-1/evidence.md), [kiểm kê AWS](rubric_cloud_audit.md) và [IaC](../edai-2/iac.md).

Runbook dưới đây tập trung EC2/S3 và thu bằng chứng pipeline. Dựng EKS và phát hành image theo [Cloud setup](cloud_setup.md). Dùng cấu hình máy, region và IP thực của lần chạy; preset cũ trong ví dụ không mô tả toàn bộ môi trường.

<a id="chuan-bi"></a>
## 1. Máy điều khiển và tài khoản (laptop)

Cài AWS CLI **v2**, Session Manager plugin, Terraform **≥1.6**, Ansible,
Python 3 + venv, rsync, OpenSSH theo hướng dẫn chính thức:
[AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html),
[SSM plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html),
[Terraform](https://developer.hashicorp.com/terraform/install),
[Ansible](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html).
Mở terminal ở thư mục gốc repo; các bước sau ghi rõ chỗ đổi thư mục.

```bash
aws --version
session-manager-plugin --version
terraform version
ansible-playbook --version
rsync --version
# Chỉ tạo nếu chưa có key; không ghi đè key đang dùng.
test -f ~/.ssh/platform_aws || ssh-keygen -t ed25519 -f ~/.ssh/platform_aws
aws configure sso
aws sso login --profile platform
export AWS_PROFILE=platform
export AWS_REGION=ap-southeast-1
aws sts get-caller-identity
aws service-quotas get-service-quota --service-code ec2 \
  --quota-code L-1216C47A --region ap-southeast-1
```

Điền thông tin SSO/profile của tài khoản đang dùng; nếu đã có profile đăng nhập
hợp lệ thì dùng profile đó, bỏ hai lệnh cấu hình SSO. Mong đợi: CLI v2,
plugin có phiên bản, đúng account và quota Standard On-Demand còn
ít nhất **4 vCPU** cho máy này (trừ các instance đang dùng). Lỗi hay gặp:
`ExpiredToken` → đăng nhập lại; `AccessDenied` → kiểm quyền của profile;
quota không đủ → xử lý quota trước apply. Các truy vấn chỉ đọc này chưa chứng
minh loại máy có sẵn ở AZ hoặc launch được.

<a id="terraform"></a>
## 2. Terraform: tạo hạ tầng (laptop)

```bash
cd infra/aws/terraform
cp terraform.tfvars.example terraform.tfvars
cat ~/.ssh/platform_aws.pub
# Sửa terraform.tfvars theo mẫu bên dưới rồi mới chạy tiếp.
```

```hcl
region            = "ap-southeast-1"
availability_zone = "ap-southeast-1a"
instance_type     = "t3.xlarge"
ssh_public_key    = "ssh-ed25519 THAY_BANG_NOI_DUNG_FILE_PUB"
cpu_credits       = "standard"
boot_disk_gb      = 30
data_disk_gb      = 100
enable_budget     = true
budget_email      = "THAY_BANG_EMAIL_NHAN_CANH_BAO"
budget_amount_usd = 30
force_destroy     = false
```

```bash
terraform init
terraform fmt -check
terraform validate
terraform plan -out data-platform.tfplan
# Đọc plan: một EC2, đúng region/AZ, hai S3, EBS, VPC/IAM và budget.
terraform apply data-platform.tfplan
terraform output
```

Mong đợi: apply thành công, có `instance_id`, `data_volume_id`, `raw_bucket`,
`lakehouse_bucket`, `region`. Bảo quản state local và plan trên máy riêng;
không đưa state/secret vào repo. Lỗi hay gặp: public key chưa thay, budget
thiếu email, IAM không có quyền tạo/pass role, quota hoặc AZ thiếu capacity.
Sửa nguyên nhân rồi plan lại; không hạ xuống máy 8 GiB vì overlay cần ≥16 GiB.
Budget chỉ cảnh báo **chi phí trước credit**, không khóa chi tiêu.

<a id="ansible"></a>
## 3. Ansible: cấu hình EC2, sinh nguồn và smoke (laptop)

Từ thư mục Terraform sau bước 2:

```bash
terraform output -raw instance_id
terraform output -raw region
terraform output -raw data_volume_id
terraform output -raw raw_bucket
terraform output -raw lakehouse_bucket
cd ../ansible
ansible-galaxy collection install -r requirements.yml
cp inventory.example.ini inventory.ini
cp group_vars/all.example.yml group_vars/all.yml
```

Điền các file vừa chép:

| Output / giá trị | Nơi điền |
|---|---|
| `instance_id` | `inventory.ini`: thay `INSTANCE_ID` trong `ansible_host` |
| `region` | thay `REGION` trong ProxyCommand và `all.yml`: `aws_region` |
| `data_volume_id`, `raw_bucket`, `lakehouse_bucket` | các khóa cùng tên trong `all.yml` (bucket không có `s3://`) |
| Đường dẫn repo tuyệt đối trên laptop | `repo_local`, ví dụ `/home/you/Data & AI Platform` |
| Secret ngẫu nhiên chỉ gồm chữ/số/`_`/`-` | `postgres_password` ≥16 ký tự; `lakekeeper_encryption_key` ≥32 ký tự |
| Nguồn | giữ mặc định `source_mode: scale`, `source_dataset_name: aws-scale150-seed7` (150 ngày × 20 đơn); đổi tên dataset khi đổi cấu hình nguồn |

Giữ `app_dir: /opt/platform`; key private trong inventory là `~/.ssh/platform_aws`.
Mã hóa toàn bộ biến trước khi chạy:

```bash
ansible-vault encrypt group_vars/all.yml
ansible-playbook -i inventory.ini site.yml --ask-vault-pass
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags source_data
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags smoke
```

Mong đợi: PLAY RECAP `failed=0`, runtime healthy; nguồn có manifest trên
`s3://<raw_bucket>/order_lifecycle/aws-scale150-seed7`, Walmart đã nạp PostgreSQL,
smoke đọc/ghi Iceberg qua IAM role thành công. Mặc định playbook không chạy
hai tag `source_data`/`smoke`, nên cần cả ba lệnh. Lỗi hay gặp:
`TargetNotConnected` → chờ SSM agent, kiểm role/Internet/region;
`Permission denied (publickey)` → key không khớp Terraform;
`CHANGE_ME`/secret sai định dạng → `ansible-vault edit group_vars/all.yml`;
S3 `AccessDenied` → đối chiếu bucket/role, không thêm access key MinIO.

<a id="pipeline"></a>
## 4. Pipeline → DataHub (EC2; capture trên laptop)

Sau smoke mới chạy fixture đầu cuối. DataHub chạy **pha sau**, khi đã dừng
Spark/Airflow để vừa RAM 16 GiB; không khởi động tất cả cùng lúc. Giữ cùng
`run_id`, dataset và receipt cho pipeline, publish và capture. Nguồn scale
chạy sau khi fixture đạt; proof fixture không chứng minh medium hay ≥100 GB.

Mở phiên vào EC2 qua SSM (không mở cổng SSH), rồi chạy **theo đúng thứ tự** dưới tài khoản `ubuntu`
để HOME, SDK DataHub và quickstart khớp với Ansible:

```bash
# Laptop, thư mục gốc repo
eval "$(terraform -chdir=infra/aws/terraform output -raw ssm_command)"
# Trong phiên EC2
sudo -iu ubuntu
cd /opt/platform
bash scripts/run/run_order_lifecycle_live.sh --phase source    # chỉ kiểm manifest trên S3
bash scripts/run/run_order_lifecycle_live.sh --phase airflow   # DAG dp_order_lifecycle, receipt, rồi dừng pha A
```

Tiếp theo chạy mục [5. Đo](#do), **rồi mới** bật DataHub (pha B dùng lại RAM của Spark/Airflow):

```bash
bash scripts/run/run_order_lifecycle_live.sh --phase datahub   # bật DataHub, publish lineage + assertion
```

Không dùng `--phase all` trước khi đo. Capture chạy trên **laptop** qua tunnel: terminal 2 giữ tunnel mở,
terminal 3 chụp. Capture chỉ cần repo + Chrome + aiohttp, không cần receipt trên laptop.

```bash
# Terminal 2 (laptop): tunnel gồm 8080/8081/8181 và 9002 (DataHub UI) / 8089 (GMS)
eval "$(terraform -chdir=infra/aws/terraform output -raw port_forward_command)"
# Terminal 3 (laptop)
.venv/bin/python scripts/evidence/capture_order_lifecycle_datahub_evidence.py
```

Ảnh ra `docs/images/datahub/v2/dp-order-lifecycle-{gold,silver}-{lineage,validation}.png`. Chụp xong, dừng DataHub trên EC2
(giữ volume, không `down -v`):

```bash
cd /opt/platform
DATAHUB_VERSION=v1.7.0.1 UI_INGESTION_DEFAULT_CLI_VERSION=1.7.0.14 \
DATAHUB_MAPPED_GMS_PORT=8089 DATAHUB_MAPPED_FRONTEND_PORT=9002 \
docker compose --env-file "$HOME/.datahub/quickstart/.local-secrets.env" \
  --profile quickstart -f "$HOME/.datahub/quickstart/docker-compose.yml" \
  -f docker/datahub-compose.override.yml -p datahub stop
```

Bằng chứng trên EC2: `out/receipts-v2-run/<run_id>/` (năm receipt + `datahub-mcps.json`) và
`out/receipts-v2-run/latest-run-id`. Cơ chế và giới hạn: [DataHub v2](../edai-1/reference/datahub_order_lifecycle.md#chạy-thật).

Mong đợi: DAG success, receipt đúng generation,
Lineage/Quality được publish và capture từ laptop qua tunnel. Lỗi hay gặp:
thiếu manifest → chạy tag source_data; sai `source_root` → kiểm prefix S3/dataset;
DataHub OOM → kiểm Spark/Airflow đã dừng; capture lỗi → kiểm tunnel/UI và receipt
cùng run. Chỉ ghi nhận kết quả sau khi kiểm output và ảnh thực của cùng run.

<a id="do"></a>
## 5. Đo trên bảng thật (EC2)

Chạy trên EC2 **sau** pha airflow và **trước** pha datahub (runner từ chối chạy khi DataHub đang bật):

```bash
cd /opt/platform
bash scripts/run/run_aws_measurements.sh --what skew          # baseline vs salted
bash scripts/run/run_aws_measurements.sh --what maintenance   # compact/z-order bảng thật
bash scripts/run/run_aws_measurements.sh --what warehouse     # EXPLAIN trước/sau index n=5
```

Kết quả: `out/aws-evidence/<ngày UTC>/{skew,maintenance,warehouse}-<timestamp>-<pid>/` (JSON, image ID, log, event log;
skew có thêm Parquet). Bản gốc event log ở `/mnt/platform-data/spark-events/aws-evidence/`. Lấy về laptop để đưa vào `docs/evidence/`:

```bash
# Trong phiên EC2: đóng gói
tar czf /tmp/platform-aws-evidence.tgz -C /opt/platform out/aws-evidence out/receipts-v2-run
# Laptop (dùng chính ProxyCommand SSM của inventory)
scp -i ~/.ssh/platform_aws -o ProxyCommand="aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p --region ap-southeast-1" \
  ubuntu@$(terraform -chdir=infra/aws/terraform output -raw instance_id):/tmp/platform-aws-evidence.tgz .
```

Mong đợi: baseline/salted có bag equality cùng input; maintenance giữ nguyên
dữ liệu và oracle sau chạy; EXPLAIN trước/sau index dùng cùng input và truy
vấn có kết quả. Lỗi hay gặp: OOM → giảm khối lượng/chạy từng pha và ghi đúng
phạm vi; CPU chậm khi hết credit standard → kiểm CPU credit, không tự bật
unlimited. Không dùng số thời gian harness làm phần trăm cải thiện index.

<a id="anh"></a>
## 6. Tunnel UI và thu evidence (laptop)

Từ thư mục gốc repo, lấy rồi chạy đúng lệnh SSH in ra trong terminal riêng:

```bash
cd infra/aws/terraform
terraform output -raw port_forward_command
# Chạy nguyên lệnh output; giữ terminal tunnel mở.
```

Output dùng key `~/.ssh/platform_aws` và ProxyCommand SSM. Airflow:
`http://127.0.0.1:8081`, Spark: `http://127.0.0.1:8080`, Lakekeeper:
`http://127.0.0.1:8181`. Output tunnel hiện có thêm DataHub UI
`http://127.0.0.1:9002` và GMS `http://127.0.0.1:8089`; kiểm output thực tế
có hai port này trước capture. Chỉ UI của pha đang chạy mới truy cập được.
Mong đợi: mở UI qua loopback; lỗi port bận → tắt tunnel cũ; lỗi SSH → kiểm
profile SSM/key; UI unavailable → kiểm đúng pha và healthcheck.

Lưu ảnh thật vào `docs/images/aws/` khi chụp; chưa tạo link ảnh chưa tồn tại.
Mỗi ảnh ghi ngày, run_id, dataset, cỡ input, instance và đường dẫn receipt.

| Thành phần | Ảnh / artifact cần giữ |
|---|---|
| Hạ tầng AWS | `terraform apply`, Console EC2/VPC/IAM + S3, region và security group; xem [IaC](../edai-2/iac.md) |
| Provisioning Ansible | PLAY RECAP `failed=0`, runtime EC2 healthy |
| Spark baseline / skew | Spark UI stage/task baseline và salted, JSON thời gian/skew/bag equality cùng input |
| Pipeline Airflow | Airflow Graph/Grid và log Validate, receipt Bronze/Silver/Gold/feature cùng run |
| Governance DataHub | DataHub Lineage + Quality trên bảng v2/feature, assertion đúng receipt; không dùng proof DP3 cũ |
| Iceberg maintenance | Bảng nghiệp vụ thật trước/sau maintenance, file/partition và oracle dữ liệu |
| Warehouse indexing | EXPLAIN ANALYZE trước/sau index, input/truy vấn giống nhau, có dòng trả về |
| Schema và model dữ liệu | Schema inventory/ERD, timestamp feature, dimension/fact/SCD2 nếu đã có proof v2 tương ứng |

<a id="dung-don"></a>
## 7. Dừng khi nghỉ, hủy môi trường (laptop)

Tại `infra/aws/terraform`, sau khi chép receipt/log/ảnh cần giữ về laptop:

```bash
aws ec2 stop-instances --region ap-southeast-1 \
  --instance-ids "$(terraform output -raw instance_id)"
aws ec2 wait instance-stopped --region ap-southeast-1 \
  --instance-ids "$(terraform output -raw instance_id)"
# Buổi sau (instance ID và key không đổi; SSM cần thời gian online lại):
aws ec2 start-instances --region ap-southeast-1 \
  --instance-ids "$(terraform output -raw instance_id)"
# Khi hủy môi trường: đọc plan hủy, chỉ xác nhận sau khi đã sao lưu dữ liệu cần giữ.
terraform plan -destroy
terraform destroy
```

Mong đợi khi nghỉ: EC2 stopped, không tính compute; **EBS và S3 vẫn tính phí**.
Public IPv4 tự cấp được thu hồi khi stop; cấu hình không có Elastic IP.
Destroy xóa cả EBS dữ liệu, S3 còn object/version sẽ chặn vì `force_destroy=false`.
Nếu destroy báo bucket không rỗng: kiểm sao lưu và mọi version/delete marker,
rồi mới quyết định xóa dữ liệu; không chỉ `aws s3 rm` rồi coi đã hết version.
Sau hủy, kiểm Console EC2/EBS/S3/Budgets và billing; lỗi IAM/state → khắc phục
với đúng profile/state, không xóa state để bỏ qua tài nguyên.

<a id="chi-phi"></a>
## 8. Ước lượng chi phí

Dùng [AWS Pricing Calculator](https://calculator.aws/) với Singapore, Linux
On-Demand, `t3.xlarge`, EBS gp3 **30 + 100 GiB**, public IPv4 theo giờ chạy,
S3 Standard theo dung lượng/version/request và data transfer thực tế.
Giá phải đối chiếu tại [EC2](https://aws.amazon.com/ec2/pricing/on-demand/),
[EBS](https://aws.amazon.com/ebs/pricing/), [S3](https://aws.amazon.com/s3/pricing/)
và [IPv4](https://aws.amazon.com/vpc/pricing/) trước apply.

**Ước lượng ngân sách, chưa phải báo giá AWS đã kiểm trực tuyến:** giả sử
compute $0,25/giờ, IPv4 $0,005/giờ, gp3 $0,10/GiB-tháng, S3 $0,025/GB-tháng.
Chạy 40 giờ/tháng + 130 GiB EBS giữ cả tháng + 20 GB S3 ≈ **$23,70/tháng**;
để EC2 chạy 730 giờ ≈ **$199,65/tháng** với cùng giả định. Cần cộng request,
transfer, version S3, thuế và biến động giá; thay đơn giá giả định bằng giá
trang AWS ở thời điểm chạy. Không truy cập mạng trong lượt viết tài liệu này.
Credit chỉ bù khoản đủ điều kiện, không làm chi phí bằng 0; budget $30 là
cảnh báo, không tự dừng máy. Chưa có lịch tự động stop.

## Bật lại EKS từ 0 node

Sau khi đưa node group về 2, làm theo thứ tự rồi mới deploy:

1. **Valkey của Substrate:** pod đổi IP nhưng cụm vẫn nhớ IP cũ ⇒ `cluster_state:fail`, `ate-api` không Ready, `kagent-controller` `CrashLoopBackOff`. Sửa không mất dữ liệu (cụm không đặt mật khẩu), rồi kiểm `cluster_state:ok`, đủ 16384 slot; hai pod kia tự hồi phục:
   ```bash
   K="kubectl --context platform-apps -n ate-system"
   IPS=$($K get pods -o wide --no-headers | awk '/^valkey-cluster/{print $6}')
   for p in 0 1 2 3 4 5; do for ip in $IPS; do $K exec valkey-cluster-$p -- valkey-cli cluster meet "$ip" 6379; done; done
   $K exec valkey-cluster-0 -- valkey-cli cluster info | head -3
   ```
2. **IP node đổi:** chạy lại Ansible với `-e feast_redis=<IP node>:30379 -e rag_pg=<IP node>:30432`, nếu không `.env` EC2 vẫn trỏ IP cũ. NodePort vào node nào cũng tới được.
3. **LLM:** `kubectl --context platform-apps -n llm scale deploy/qwen --replicas=1` rồi đợi rollout trước khi chạy eval; thiếu LLM thì eval ra 0 điểm.
4. **Jenkins:** dùng `scripts/setup/setup_jenkins.sh` thay vì `docker compose up jenkins` (thiếu `DOCKER_GID`, kubeconfig); `setup_vault.sh` để unseal Vault trước. Code của job lấy từ `/repo/out/platform.bundle`: tạo lại bằng `git bundle create out/platform.bundle main` rồi chép lên EC2.
