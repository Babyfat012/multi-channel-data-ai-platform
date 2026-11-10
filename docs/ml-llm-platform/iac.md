# Infrastructure as Code on AWS

Building the AWS infrastructure with code (Terraform) and configuring machines with code (Ansible).

**Conclusion:** `terraform plan` against the real infrastructure reports **No changes**, meaning the running infrastructure fully matches the code.
The Ansible playbook run on EC2 finished with `failed=0`.

The results below come from runs on AWS; the docs do not claim the services are still up.

## Deployment scope

The infrastructure is built in the Sydney region (`ap-southeast-2`) and includes: the EC2 instance running the Data Platform, two S3 buckets (raw and lakehouse), ECR and the EKS cluster `platform-apps`.
The EC2 instance and the two EKS workers use the `r6i.xlarge` instance type. Numbers from older runs still record the instance type at the time of measurement.

| Component | Code and responsibility |
|---|---|
| Terraform Data Platform | [Root module](../../infra/aws/terraform/README.md): VPC, EC2/EBS, IAM, S3, ECR and the network used by EKS |
| Terraform EKS | [Root module](../../infra/aws/kubernetes/README.md): cluster, nodes, identity and workload permissions |
| Ansible | [Playbook/roles](../../infra/aws/ansible/README.md): base, data disk, app, DataHub, source data and smoke on EC2 |

Applications on EC2 get AWS permissions through an IAM role; applications on EKS get them through Pod Identity. No secrets live in the repository. Order of operations: [Cloud setup](../shared/cloud_setup.md), [AWS runbook](../shared/aws_deploy.md).

## Terraform run

Ran `terraform plan` against the running infrastructure. Terraform reads back the real state of each resource (VPC, subnet, security group, EC2, EBS,
S3 raw/lakehouse, IAM role and the `platform-ci-ecr` policy, ECR repo, budget) and compares it with the code.

The result is **No changes**: no resource differs from the code. This still holds after an earlier incident:
Terraform used to replace the EC2 instance when it was stopped; this was fixed with `lifecycle.ignore_changes`.

![terraform plan: refreshes each AWS resource, No changes](../evidence/aws/terraform-plan-no-changes-20261031.png)

## Ansible run

Ran the whole `site.yml` playbook against EC2 through SSM (Session Manager).
The command passes the current EKS node IPs for Redis and pgvector with `-e feast_redis=… -e rag_pg=…`. Without these two parameters, the `.env` file falls back to the old default IPs.

The screenshot below shows the tasks of the `app` role (Dockerfile checksum check, Fernet key generation, compose up, MLflow, Lakekeeper, Airflow Variables) and the `datahub` role.
The final result (PLAY RECAP) has 4 `changed` tasks; 2 of them are visible in the screenshot (starting the Data Platform services, starting MLflow).
Most tasks are `ok` because this is a rerun with nothing to change.

![Ansible site.yml: app/datahub role tasks, PLAY RECAP ok=51 changed=4 failed=0](../evidence/aws/ansible-play-recap-20261031.png)

The deployment evidence is the `terraform plan` screenshot (comparing code with the real infrastructure), not `terraform validate` (which only checks code syntax).
How to run the pipeline and take screenshots for each Data Platform item is in the [evidence table](../shared/aws_deploy.md#anh).

Operations:

- When not in use, [stop EC2](../shared/aws_deploy.md#dung-don). EBS and S3 are still billed while the instance is stopped.
- To tear the environment down completely: back up first, then run `terraform destroy`, then check in the Console for remaining object versions in the buckets and leftover resources.

<details>
<summary>Reference: design, development guide and records from before the AWS deployment</summary>

Local records keep their original environment and are not a substitute for cloud evidence.

<!-- Purpose: describe the AWS Terraform/Ansible, security decisions and where the IaC proof for measurements lives. -->
## What Terraform creates and why

| Resource | Configuration / security decision |
|---|---|
| VPC, public subnet, Internet Gateway, route and association | Dedicated VPC `10.42.0.0/16`, subnet `10.42.0.0/24`. Egress is used to pull images and call SSM and S3. The current configuration uses a public IPv4 instead of a NAT Gateway |
| Security group | **No inbound connections (ingress)**, only outbound IPv4; no SSH or UI exposed to the Internet. The SSM agent on the machine calls out to AWS, so SSH goes through Session Manager |
| EC2 + key pair | Ubuntu 24.04 Canonical, `t3.xlarge` 4 vCPU/16 GiB; only the public key goes into Terraform. The private key stays on the laptop; `standard` CPU credits avoid surplus charges |
| EC2 metadata | **IMDSv2** required. Hop limit set to **2** so containers (behind one more network layer) can still get credentials; no access keys embedded in Compose |
| EBS root + EBS data/attachment | gp3, default 30 + 100 GiB, **encrypted=true**. A separate data disk keeps the data when EC2 is replaced; `terraform destroy` still deletes the disk. EC2/EBS in the same AZ |
| IAM role, inline policy, instance profile | EC2 receives temporary credentials; list/location permissions on the two buckets and read/write/delete on objects in those two buckets only. No account-wide S3 permissions; `AmazonSSMManagedInstanceCore` attached for SSM |
| Two S3 buckets `raw`, `lakehouse` | Names include the account ID to avoid global collisions; **block public** with all four options, `BucketOwnerEnforced` disables ACLs, access through IAM |
| S3 versioning + lifecycle | Versions kept; noncurrent versions deleted after 30 days, `tmp/` after 7 days and incomplete multipart uploads after 7 days. Do not put primary data under `tmp/`; there is no bucket-wide rule deleting current Iceberg data |
| Budget (enabled in the runbook) | Email when exceeding 50/90/100% of the budget; credits are not subtracted when computing alerts. Not a hard limit or a shutdown schedule |

To use SSM, the operator's AWS profile must have permission to start sessions, and the SSM agent on EC2 must be online.

Notes:

- No inbound connections does not mean EC2 cannot reach the Internet: the subnet has a route through the Internet Gateway and the machine has a public IPv4.
- Ansible and the applications only open UI ports on loopback (`127.0.0.1`). To view a UI, open an SSH tunnel following the Terraform output.
- The IAM role can delete objects because Iceberg needs to delete files when managing tables. Versioning only keeps old versions for a limited time, so it is not a substitute for backups.

## Structure and Ansible roles

```text
infra/aws/
├── terraform/
│   ├── versions.tf, variables.tf       # provider, region, instance config quota
│   ├── network.tf, compute.tf          # network, EC2, EBS, IMDSv2
│   ├── iam.tf, storage.tf, budget.tf   # role, S3, cost alerts
│   ├── outputs.tf                     # ID/bucket/region and SSM tunnel
│   └── terraform.tfvars.example
└── ansible/
    ├── site.yml, requirements.yml
    ├── inventory.example.ini
    ├── group_vars/all.example.yml
    └── roles/                         # base, data_disk, app, datahub, source_data, smoke
```

| Role | What it does |
|---|---|
| `base` | Checks Ubuntu 24.04; installs Docker/Compose, AWS CLI v2, Python venv/rsync; adds the user to the docker group |
| `data_disk` | Finds the EBS volume by NVMe volume ID, checks/mounts the disk at `/mnt/platform-data`; does not rely on the `/dev/sdf` name on Nitro |
| `app` | Rsyncs the repo excluding secrets/local data; renders `.env` with mode 0600, generates Fernet, builds driver/executor with the same Python; starts PostgreSQL/Lakekeeper/Spark/Airflow, bootstraps the S3 warehouse and Airflow Variables; venv `/mnt/platform-data/venv` |
| `datahub` (default `enable_datahub: true`) | Prepares uv, the CLI `acryl-datahub==1.7.0.14`, the Compose quickstart and its own secrets with mode 0600; **does not start DataHub**. The CLI is installed for the SSH user; the live phase runs after stopping Spark/Airflow |
| `source_data` (separate tag) | Generates fixtures or scales by dataset name; syncs to S3 without `--delete`, excluding `_truth/`; loads Walmart into PostgreSQL |
| `smoke` (separate tag) | Checks the runtime and container identity, reads/writes S3/Iceberg before the pipeline |

## How to run and collect proof

From the repo root, prepare the AWS profile, tools and key following
[preparation](../shared/aws_deploy.md#chuan-bi), then:

```bash
cd infra/aws/terraform
cp terraform.tfvars.example terraform.tfvars
## Fill in the public key, region/AZ for the Sydney environment, enable_budget=true and a real email.
terraform init
terraform plan -out data-platform.tfplan
terraform apply data-platform.tfplan
terraform output
cd ../ansible
ansible-galaxy collection install -r requirements.yml
cp inventory.example.ini inventory.ini
cp group_vars/all.example.yml group_vars/all.yml
## Fill in the instance ID/region in the inventory; bucket/volume/repo/secrets in all.yml.
ansible-vault encrypt group_vars/all.yml
ansible-playbook -i inventory.ini site.yml --ask-vault-pass
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags source_data
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags smoke
```

Expected result: Terraform creates exactly the resources in the table; PLAY RECAP has `failed=0` and the smoke step succeeds.
Common errors (IAM, quota, SSM, key, secrets) and how to handle them: [Terraform](../shared/aws_deploy.md#terraform), [Ansible](../shared/aws_deploy.md#ansible).

Local state, plan files, inventory, `.env`, secrets and the vault password must not be published.
Only verified screenshots and receipts (result receipt files) go in `docs/`.

</details>

[Section index](README.md) · [Main index](../README.md)
