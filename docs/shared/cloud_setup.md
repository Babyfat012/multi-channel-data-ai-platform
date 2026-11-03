# Chuẩn bị và triển khai cloud

Đích triển khai là AWS cho cả Data Platform, ML Platform và LLM Platform. Người vận hành trực tiếp chạy các lệnh AWS, Terraform plan/apply và Ansible. Những kiểm tra offline của repo không xác nhận đã có tài nguyên hoặc workload chạy trên AWS.

Cấu hình demo và ngân sách ghi các preset tài nguyên và lịch bật/tắt. Cấu hình đã ghi nhận ngày 31/10: EC2 Data Platform và hai EKS worker `r6i.xlarge`, tổng 12 vCPU; xem [kiểm kê AWS](rubric_cloud_audit.md).

## 1. Kiểm tài khoản trước khi tạo tài nguyên

Cần AWS CLI v2 đã đăng nhập bằng profile của bạn, Terraform, SSM plugin, Docker, kubectl, Helm, SSH/rsync và Ansible. Kiểm tra các công cụ này trên máy điều khiển trước khi chạy. Cài Ansible trong môi trường riêng theo [hướng dẫn chính thức](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html).

```bash
# Thay TEN_PROFILE bằng profile đã đăng nhập; không gửi access key/mật khẩu.
export AWS_PROFILE=TEN_PROFILE
export AWS_REGION=ap-southeast-2
python3 scripts/evidence/check_aws_readiness.py --profile "$AWS_PROFILE" --region "$AWS_REGION"
```

Script chỉ gọi API đọc: identity, account plan, quota Standard vCPU, thông số/offering EC2 và phiên bản EKS. Receipt riêng tư tại `out/cloud-preflight/` có quyền `0600`; lỗi đăng nhập dừng trước các probe dịch vụ khác. `AccessDenied` được ghi là thiếu kiểm chứng, không tự đổi quyền hoặc profile.

Trước plan, chốt account/role, region, hai AZ, ngân sách gồm phần ngoài credit, loại/số node, phiên bản Kubernetes và IP egress quản trị. Quota là hạn mức, chưa trừ tài nguyên đang dùng; offering không đảm bảo có capacity. Giá, credit còn lại và quyền create vẫn cần kiểm trên tài khoản của bạn. Tham khảo [giá EKS](https://aws.amazon.com/eks/pricing/), [EC2](https://aws.amazon.com/ec2/pricing/) và [ECR](https://aws.amazon.com/ecr/pricing/).

## 2. Data Platform, mạng và registry

Root [Terraform Data Platform](../../infra/aws/terraform/README.md) quản lý EC2/EBS/S3. Các phần mới đều opt-in:

- `enable_ecr = true`: chín repository image trong namespace dự án, immutable tag, scan-on-push, không force-delete image khi destroy.
- `enable_kubernetes_network = true`: thêm subnet tại AZ thứ hai. Hai public subnet cung cấp egress cho node; không tạo NAT Gateway. Node có public IP, chưa có ingress Internet cho ứng dụng.
- `secondary_availability_zone`: bắt buộc khác AZ đầu và cùng region.

```bash
cd infra/aws/terraform
cp terraform.tfvars.example terraform.tfvars
# Điền SSH PUBLIC key, region/AZ, instance và ngân sách/email.
# Bật enable_ecr, enable_kubernetes_network và điền secondary_availability_zone.
terraform init
terraform fmt -check
terraform validate
terraform plan -out data-platform.tfplan
# Đọc plan, đối chiếu account/region và mọi create/replace/delete trước khi chạy tiếp.
terraform apply data-platform.tfplan
terraform output
cd ../../..
```

Không commit tfvars, state hoặc plan. Nếu đã có state cũ, xem hướng dẫn di chuyển trong Terraform README trước khi apply. Dựng runtime EC2 theo [Ansible](../../infra/aws/ansible/README.md); vẫn cần điền inventory/biến thật và mã hóa secret bằng Ansible Vault.

## 3. Kubernetes cho ML Platform / LLM Platform

Root [Terraform Kubernetes](../../infra/aws/kubernetes/README.md) đọc outputs của state Data Platform. Root này tạo EKS thật và có chi phí riêng khi apply.

```bash
cd infra/aws/kubernetes
cp terraform.tfvars.example terraform.tfvars
# Điền data_platform_state, kubernetes_version đang được hỗ trợ,
# admin_role_arn thường trực, api_allowed_cidrs và node_instance_types đã chốt.
terraform init
terraform fmt -check
terraform validate
terraform plan -out kubernetes.tfplan
# Review số node, disk, role và endpoint trước khi apply.
terraform apply kubernetes.tfplan
terraform output -raw kubeconfig_command
# Chạy lệnh update-kubeconfig vừa được in; alias mặc định là platform-apps.
cd ../../..
export KCTX=platform-apps
kubectl --context "$KCTX" get nodes
kubectl --context "$KCTX" -n kube-system get pods
kubectl --context "$KCTX" -n kube-system rollout status daemonset/eks-pod-identity-agent --timeout=180s
kubectl --context "$KCTX" -n kube-system rollout status deployment/ebs-csi-controller --timeout=180s
```

Cluster API luôn có endpoint private; endpoint public giới hạn CIDR quản trị `/24` hoặc hẹp hơn. Quyền admin gắn IAM role cụ thể, không tự cấp cho mọi principal tạo cluster. AWS profile dùng khi chạy kubectl phải tương ứng role đó; nếu dùng profile khác, cấu hình assume-role được phép trước khi lấy token. Workload lấy credential qua Pod Identity; node không có S3 data policy.

Kiểm storage class hiện có trước khi chọn default. Nếu đã có default class khác, đổi annotation của class cũ về `false`, rồi áp class gp3 của repo:

```bash
kubectl --context "$KCTX" get storageclass
# Chỉ khi gp2 hiện là default:
# kubectl --context "$KCTX" annotate storageclass gp2 storageclass.kubernetes.io/is-default-class=false --overwrite
kubectl --context "$KCTX" apply -f infra/k8s/storage-aws.yaml
```

`gp3` dùng EBS CSI và `WaitForFirstConsumer`; PVC cần pod sử dụng mới bind. PV dùng `Retain`: xóa workload/PVC/cluster không tự đảm bảo đã dọn hết EBS. Cần lưu ý cả dữ liệu và chi phí khi dọn môi trường.

## 4. Phát hành image và chọn context cloud

```bash
export REGISTRY=$(terraform -chdir=infra/aws/terraform output -raw image_registry)
export ECR_HOST=${REGISTRY%%/*}
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_HOST"
export TAG="$(git rev-parse HEAD)-BUILD_ID"  # thay BUILD_ID cho mỗi build mới
# Ví dụ build/push Feature API; các image khác dùng cùng helper và namespace.
docker build -f docker/Dockerfile.serving -t "platform-feature-api:$TAG" .
scripts/run/push_images.sh "platform-feature-api:$TAG"
```

Tag ECR bất biến: không push đè tag đã phát hành. Lưu commit SHA, build ID và digest khi thu evidence. Các script deploy nhận `KCTX`; script bootstrap `setup_kind_*` vẫn dành cho local và không chạy với EKS. Jenkins có tham số registry/context/kubeconfig credential, nhưng pipeline Compose cho RAG/stream vẫn cần cấu hình AWS riêng trước khi chạy toàn chuỗi.

## 5. Feature API sau khi dependency đã sẵn sàng

Chỉ thực hiện khi Redis, pgvector, registry Feast trên S3 và KServe đã được triển khai/kiểm tra. Đây là bước kiểm tích hợp tiếp theo, không phải hệ quả tự động của Terraform apply.

```bash
export PLATFORM_STORAGE=s3
export KCTX=platform-apps
export LAKEHOUSE_BUCKET=$(terraform -chdir=infra/aws/terraform output -raw lakehouse_bucket)
# POSTGRES_USER / POSTGRES_PASSWORD lấy từ nguồn secret của môi trường, không ghi vào Git.
SKIP_BUILD_PUSH=1 bash scripts/setup/deploy_feature_api.sh "$TAG"
kubectl --context "$KCTX" -n feature-store rollout status deployment/feature-api --timeout=180s
```

Chart AWS dùng ClusterIP và ServiceAccount `feature-api` tại namespace `feature-store`, khớp Pod Identity. Secret không chứa key MinIO/AWS tĩnh. Helm `--atomic` rollback nếu pod không Ready. Gateway/ingress được triển khai riêng; `setup_ingress.sh` cần `INGRESS_DOMAIN` ở context cloud, không suy IP từ container kind. CA nội bộ trong script chưa thay cho cấu hình ACME/domain thật.

## 6. Gate để chuyển sang chạy pipeline

- EC2 truy cập được qua SSM; runtime Data Platform healthy; S3/Iceberg smoke đạt.
- ECR push và node pull đúng digest; node Ready; Pod Identity/EBS CSI hoạt động; PVC smoke có kiểm đọc/ghi.
- Đường kết nối EC2 ↔ EKS được kiểm thực tế. Terraform chỉ mở các cổng EC2 đã cho phép từ SG cluster; Compose Data Platform hiện bind loopback. Cần cấu hình listener private và endpoint của các service trước tích hợp, không mở UI/database ra Internet.
- MLflow/KFP/KServe, Redis/pgvector, LLM/agent, Vault, ingress và observability trên cloud vẫn cần deploy workload sau khi nền hạ tầng đạt. Các script training hiện còn nhánh credential MinIO; chưa coi là cloud-ready chỉ vì đã nhận `KCTX`.
- Thu receipt thực tế vào `out/`; chỉ đưa bản đã kiểm vào [evidence](../edai-1/evidence.md). Không cập nhật trạng thái “đã chạy cloud” từ kiểm schema hoặc render Helm.

Nguồn đối chiếu: [EKS networking](https://docs.aws.amazon.com/eks/latest/best-practices/subnets.html), [node IAM role](https://docs.aws.amazon.com/eks/latest/userguide/create-node-role.html), [Pod Identity trust](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-role.html), [EBS CSI](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html).

[Mục lục tài liệu](../README.md)
