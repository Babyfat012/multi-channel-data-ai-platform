#!/usr/bin/env bash
# Add this machine's current public IP to the list of IPs allowed to call the EKS API (public_access_cidrs), then wait for the cluster to be ACTIVE.
# Rerunnable at any time: does nothing if the IP is already there; the existing list is read from AWS (not hard-coded).
# Use when `kubectl` reports "dial tcp …:443: i/o timeout" because the ISP changed your IP. This change drifts from Terraform (terraform.tfvars).
# Run: scripts/setup/eks_allow_my_ip.sh        Preview without changing anything: DRY_RUN=1 scripts/setup/eks_allow_my_ip.sh
set -euo pipefail
cluster=${EKS_CLUSTER:-platform-apps}
region=${AWS_REGION:-ap-southeast-2}
my_ip=$(curl -fsS --max-time 10 https://checkip.amazonaws.com | tr -d '[:space:]')
[[ "$my_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Could not get the public IP: '$my_ip'" >&2; exit 1; }
aws eks wait cluster-active --name "$cluster" --region "$region"
current=$(aws eks describe-cluster --name "$cluster" --region "$region" \
  --query 'cluster.resourcesVpcConfig.publicAccessCidrs' --output text | tr '\t' ',')
echo "Current IP: $my_ip"
case ",$current," in
  *",$my_ip/32,"*) echo "IP is already in the allow list; no change needed."; exit 0 ;;
esac
echo "New list: $current,$my_ip/32"
[[ -z "${DRY_RUN:-}" ]] || { echo "(DRY_RUN: not sent to AWS)"; exit 0; }
aws eks update-cluster-config --name "$cluster" --region "$region" \
  --resources-vpc-config "publicAccessCidrs=$current,$my_ip/32" --query 'update.status' --output text
aws eks wait cluster-active --name "$cluster" --region "$region"
echo "Done. Try: kubectl --context $cluster get nodes"
