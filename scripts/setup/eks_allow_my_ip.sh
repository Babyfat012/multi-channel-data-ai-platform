#!/usr/bin/env bash
# Thêm IP công khai hiện tại của máy này vào danh sách IP được gọi API EKS (public_access_cidrs), rồi đợi cluster ACTIVE.
# Chạy lại được bất cứ lúc nào: IP đã có thì không làm gì; danh sách cũ được đọc từ AWS (không ghi cứng).
# Dùng khi `kubectl` báo "dial tcp …:443: i/o timeout" vì IP nhà mạng đổi. Thay đổi này lệch với Terraform (terraform.tfvars).
# Chạy: scripts/setup/eks_allow_my_ip.sh        Xem trước, không đổi gì: DRY_RUN=1 scripts/setup/eks_allow_my_ip.sh
set -euo pipefail
cluster=${EKS_CLUSTER:-platform-apps}
region=${AWS_REGION:-ap-southeast-2}
my_ip=$(curl -fsS --max-time 10 https://checkip.amazonaws.com | tr -d '[:space:]')
[[ "$my_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Không lấy được IP công khai: '$my_ip'" >&2; exit 1; }
aws eks wait cluster-active --name "$cluster" --region "$region"
current=$(aws eks describe-cluster --name "$cluster" --region "$region" \
  --query 'cluster.resourcesVpcConfig.publicAccessCidrs' --output text | tr '\t' ',')
echo "IP hiện tại: $my_ip"
case ",$current," in
  *",$my_ip/32,"*) echo "IP đã nằm trong danh sách cho phép; không cần đổi."; exit 0 ;;
esac
echo "Danh sách mới: $current,$my_ip/32"
[[ -z "${DRY_RUN:-}" ]] || { echo "(DRY_RUN: chưa gửi lên AWS)"; exit 0; }
aws eks update-cluster-config --name "$cluster" --region "$region" \
  --resources-vpc-config "publicAccessCidrs=$current,$my_ip/32" --query 'update.status' --output text
aws eks wait cluster-active --name "$cluster" --region "$region"
echo "Xong. Thử: kubectl --context $cluster get nodes"
