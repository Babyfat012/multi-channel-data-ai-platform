<!-- EKS root for ML/LLM: the operator runs plan/apply, independent of the Data Platform root. -->
# Kubernetes AWS — ML Platform / LLM Platform

This root creates EKS, a managed node group, admin access via an Access Entry, the EKS Pod Identity Agent, EBS CSI and a Pod Identity that reads the feature store for ServiceAccount `feature-store/feature-api`. It does not yet deploy Kubeflow, KServe, Redis, pgvector, LLMs, ingress or agents.

It reads the local state of `../terraform` after that root is applied with `enable_kubernetes_network=true` and two AZs. Do not copy state to Git. `kubernetes_version`, `admin_role_arn`, the admin CIDR and node types are required inputs; the operator must check quota, supported versions and budget before plan.

```bash
cp terraform.tfvars.example terraform.tfvars
# Fill in real values; the state path is relative to this root directory.
terraform init
terraform fmt -check
terraform validate
terraform plan -out kubernetes.tfplan
# Review the plan before applying; apply creates billable AWS services.
terraform apply kubernetes.tfplan
terraform output -raw kubeconfig_command
```

- The API is private for nodes; the public side accepts only specific admin IPs. The profile used with kubectl must have the identity matching `admin_role_arn`. Nodes sit in a public subnet for egress; they get no SSH key and expose no applications to the Internet.
- The node group uses On-Demand x86_64 with encrypted root disks. A single-node group may be disrupted during updates; sizing and node count must be settled per workload.
- EBS CSI uses the IAM policy `AmazonEBSCSIDriverPolicyV2`. Pod Identity is bound to cluster, namespace and ServiceAccount via request tags.
- The Feature API can only read the `feature-store/` prefix of the lakehouse bucket. This role does not yet grant training/MLflow writes or Feast registry publishing.
- By default only PostgreSQL/Lakekeeper are opened from the cluster SG to the EC2 SG. Loopback Compose listeners need their own private configuration before integration; do not open host ports ad hoc.
- EKS state must be handled before destroying the EC2/VPC root, because subnets, security groups, volumes and dependencies remain. Preserve data/PVs and check remaining resources before cleanup.

Full procedure and gates: [cloud setup](../../../docs/shared/cloud_setup.md).

## Presets for a fixed 16 vCPU quota

`demo.tfvars.example` (the chosen preset) sets two c6i.xlarge workers for machine-level redundancy; `demo-peak.tfvars.example` sets one m6i.2xlarge worker. Both allow desired/min to drop to 0 when idle, use the MINIMAL update strategy (the 16 vCPU quota is fully used) and place workers in the first subnet to keep the EBS AZ. Workloads may be disrupted during updates or scale-to-zero; the Kubernetes control plane keeps running and is billed. Do not stop the managed node group's EC2 instances by hand. Check the vCPU quota and budget before applying a preset.
