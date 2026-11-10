<!-- Purpose: entry point for the AWS Data Platform Terraform: what it builds, how to run it, what it costs. -->
# Terraform AWS Data Platform

Terraform ≥1.6, provider `hashicorp/aws` `~> 6.0` (exact version pinned in
`.terraform.lock.hcl`). State is local. Milestone 1 **has no EKS yet**.

Builds: VPC `10.42.0.0/16` + 1 public subnet + Internet Gateway, a security group
with **no inbound ports**, one `t3.xlarge` Ubuntu 24.04 EC2, one gp3 data EBS,
an IAM role (instance profile, no access key), 2 S3 buckets (`raw`, `lakehouse`),
an optional budget. Diagram: [`docs/diagrams/data-platform-system-architecture-excalidraw.png`](../../../docs/diagrams/data-platform-system-architecture-excalidraw.png).

Before running, check the AWS profile's permissions, quota and the instance/region settings of the deployment account per the [runbook](../../../docs/shared/aws_deploy.md). The repo has no evidence yet of an EC2 launch on AWS.

## Run

```bash
cd infra/aws/terraform
cp terraform.tfvars.example terraform.tfvars   # fill in ssh_public_key
terraform init
terraform plan -out data-platform.tfplan
terraform apply data-platform.tfplan
```

Your machine needs a logged-in `aws` CLI and `session-manager-plugin` (for SSH over SSM).
Then continue with [Ansible](../ansible/README.md).

## Cost notes

- EC2 is billed only while running: `aws ec2 stop-instances --instance-ids <id>` when idle.
- **EBS is still billed while the EC2 is stopped.** The auto-assigned public IPv4 in this setup is
  released by AWS on stop; there is no Elastic IP. See [EC2 IP rules](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html).
- `cpu_credits = "standard"`: when CPU credits run out the machine slows down, with no extra charge.
- The budget only alerts; it does not block spending. Enable it with `enable_budget = true` + `budget_email`.
- `terraform destroy` also deletes the data EBS. Buckets that still hold data block destroy
  unless `force_destroy = true`.

## Differences from GCP

| GCP (`infra/gcp`) | AWS (here) |
|---|---|
| IAP + OS Login, firewall opens 22 to the IAP range | SSM, security group with no ingress |
| Service account attached to the VM | IAM role + instance profile |
| `/dev/disk/by-id/google-platform-data` | `/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol…` |
| Budget emails the Billing Admin automatically | Must set `budget_email` |
| Enable APIs (`google_project_service`) | Not needed |

## State of existing environments

Current resource addresses use `data_platform` and `operator`; AWS names use the `platform` / `data-platform` prefix. If a configuration with old resource addresses or names was already applied, check the state and plan a migration before applying. Read the full plan to tell metadata updates apart from recreating the EC2, EBS or buckets; renaming in the repo does not move state or data by itself.

## Registry and network for ML/LLM

Enable `enable_ecr` to create ECR repositories; enable `enable_kubernetes_network` and set `secondary_availability_zone` to get two subnets for the [EKS](../kubernetes/README.md) root. Both flags default to off. See [cloud setup](../../../docs/shared/cloud_setup.md) to check the account, review the plan and deploy in order.
