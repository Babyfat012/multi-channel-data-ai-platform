<!-- Purpose: operator runbook for deploying the Data Platform batch on EC2 + S3, collecting proof and cleaning up resources. -->
# AWS deployment — Data Platform

Deployed environment: Sydney `ap-southeast-2`, EC2 Data Platform/S3 and EKS `platform-apps` for ML/LLM. The runs are cross-checked in [Data Platform evidence](../data-platform/evidence.md), the [AWS validation status](aws_validation.md) and [IaC](../ml-llm-platform/iac.md).

The runbook below focuses on EC2/S3 and collecting pipeline evidence. Set up EKS and publish images following [Cloud setup](cloud_setup.md). Use the machine configuration, region and IPs of your actual run; the older presets in the examples do not describe the whole environment.

<a id="chuan-bi"></a>
## 1. Control machine and account (laptop)

Install AWS CLI **v2**, the Session Manager plugin, Terraform **≥1.6**, Ansible,
Python 3 + venv, rsync and OpenSSH following the official guides:
[AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html),
[SSM plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html),
[Terraform](https://developer.hashicorp.com/terraform/install),
[Ansible](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html).
Open a terminal at the repository root; the steps below say explicitly where to change directory.

```bash
aws --version
session-manager-plugin --version
terraform version
ansible-playbook --version
rsync --version
# Only create a key if none exists; do not overwrite a key in use.
test -f ~/.ssh/platform_aws || ssh-keygen -t ed25519 -f ~/.ssh/platform_aws
aws configure sso
aws sso login --profile platform
export AWS_PROFILE=platform
export AWS_REGION=ap-southeast-1
aws sts get-caller-identity
aws service-quotas get-service-quota --service-code ec2 \
  --quota-code L-1216C47A --region ap-southeast-1
```

Fill in the SSO/profile details of the account you use; if you already have a valid logged-in
profile, use it and skip the two SSO configuration commands. Expected: CLI v2,
the plugin reports a version, the right account, and a Standard On-Demand quota with
at least **4 vCPU** free for this machine (minus instances already running). Common errors:
`ExpiredToken` → log in again; `AccessDenied` → check the profile's permissions;
insufficient quota → resolve the quota before apply. These read-only queries do not
prove that the instance type is available in the AZ or can be launched.

<a id="terraform"></a>
## 2. Terraform: create the infrastructure (laptop)

```bash
cd infra/aws/terraform
cp terraform.tfvars.example terraform.tfvars
cat ~/.ssh/platform_aws.pub
# Edit terraform.tfvars following the template below before continuing.
```

```hcl
region            = "ap-southeast-1"
availability_zone = "ap-southeast-1a"
instance_type     = "t3.xlarge"
ssh_public_key    = "ssh-ed25519 REPLACE_WITH_PUB_FILE_CONTENTS"
cpu_credits       = "standard"
boot_disk_gb      = 30
data_disk_gb      = 100
enable_budget     = true
budget_email      = "REPLACE_WITH_ALERT_EMAIL"
budget_amount_usd = 30
force_destroy     = false
```

```bash
terraform init
terraform fmt -check
terraform validate
terraform plan -out data-platform.tfplan
# Read the plan: one EC2 instance, correct region/AZ, two S3 buckets, EBS, VPC/IAM and budget.
terraform apply data-platform.tfplan
terraform output
```

Expected: apply succeeds, with `instance_id`, `data_volume_id`, `raw_bucket`,
`lakehouse_bucket`, `region`. Keep the local state and plan on your own machine;
do not put state/secrets in the repo. Common errors: public key not replaced, budget
missing an email, IAM lacking permission to create/pass roles, quota or AZ lacking capacity.
Fix the cause and plan again; do not downsize to an 8 GiB machine, because the overlay needs ≥16 GiB.
The budget only alerts on **cost before credits**; it does not cap spending.

<a id="ansible"></a>
## 3. Ansible: configure EC2 and smoke test (laptop)

From the Terraform directory after step 2:

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

Fill in the files you just copied:

| Output / value | Where to fill it in |
|---|---|
| `instance_id` | `inventory.ini`: replace `INSTANCE_ID` in `ansible_host` |
| `region` | replace `REGION` in ProxyCommand and `all.yml`: `aws_region` |
| `data_volume_id`, `raw_bucket`, `lakehouse_bucket` | the keys with the same names in `all.yml` (buckets without `s3://`) |
| Absolute repo path on the laptop | `repo_local`, for example `/home/you/Data & AI Platform` |
| Random secrets using only letters/digits/`_`/`-` | `postgres_password` ≥16 characters; `lakekeeper_encryption_key` ≥32 characters |

Keep `app_dir: /opt/platform`; the private key in the inventory is `~/.ssh/platform_aws`.
Encrypt all variables before running:

```bash
ansible-vault encrypt group_vars/all.yml
ansible-playbook -i inventory.ini site.yml --ask-vault-pass
ansible-playbook -i inventory.ini site.yml --ask-vault-pass --tags smoke
```

Expected: PLAY RECAP `failed=0`, runtime healthy, and the smoke test reads/writes Iceberg through the IAM role
successfully. By default the playbook does not run the `smoke` tag, so both commands are needed. Common errors:
`TargetNotConnected` → wait for the SSM agent, check role/Internet/region;
`Permission denied (publickey)` → key does not match Terraform;
`CHANGE_ME`/badly formatted secret → `ansible-vault edit group_vars/all.yml`;
S3 `AccessDenied` → check bucket/role; do not add MinIO access keys.

<a id="pipeline"></a>
## 4. Run the pipeline (EC2)

Pipeline A reads the order exports already on `s3://<raw_bucket>/order_snapshots/`. Trigger `dp_nightly` from the Airflow UI
(through the tunnel in [section 6](#anh)) or run the stages one by one, as described in
[pipeline orchestration](../data-platform/pipeline_orchestration.md). DataHub lineage and assertions for dp1–dp4:
[data governance](../data-platform/data_governance.md).

<a id="do"></a>
## 5. Measure on real tables (EC2)

Bronze/Spark measurements: [Spark optimization](../data-platform/reference/spark.md); Iceberg maintenance:
[Iceberg maintenance](../data-platform/reference/iceberg_maintenance.md). The former order-lifecycle (v2) runner,
DataHub capture and measurements were removed with that pipeline (archived in git tag `v2-archive`).

<a id="anh"></a>
## 6. UI tunnel and evidence collection (laptop)

From the repository root, get and run the exact SSH command printed, in a separate terminal:

```bash
cd infra/aws/terraform
terraform output -raw port_forward_command
# Run the output command as is; keep the tunnel terminal open.
```

The output uses the `~/.ssh/platform_aws` key and the SSM ProxyCommand. Airflow:
`http://127.0.0.1:8081`, Spark: `http://127.0.0.1:8080`, Lakekeeper:
`http://127.0.0.1:8181`. The tunnel output now also includes the DataHub UI
`http://127.0.0.1:9002` and GMS `http://127.0.0.1:8089`; check that the actual output
has these two ports before capturing. Only the UIs of the running phase are reachable.
Expected: UIs open over loopback; port in use → close the old tunnel; SSH error → check
the SSM profile/key; UI unavailable → check the phase and the healthcheck.

Save real screenshots to `docs/images/aws/` when capturing; do not create links to screenshots that do not exist yet.
Each screenshot records the date, run_id, dataset, input size, instance and receipt path.

| Component | Screenshot / artifact to keep |
|---|---|
| AWS infrastructure | `terraform apply`, EC2/VPC/IAM + S3 Console, region and security group; see [IaC](../ml-llm-platform/iac.md) |
| Ansible provisioning | PLAY RECAP `failed=0`, EC2 runtime healthy |
| Spark baseline / skew | Spark UI stages/tasks for baseline and salted, JSON with timing/skew/bag equality on the same input |
| Airflow pipeline | Airflow Graph/Grid and Validate logs, Bronze/Silver/Gold/feature receipts from the same run |
| DataHub governance | DataHub Lineage + Quality on v2/feature tables, assertions matching the receipts; do not use the old DP3 proof |
| Iceberg maintenance | Real business tables before/after maintenance, files/partitions and data oracle |
| Warehouse indexing | EXPLAIN ANALYZE before/after index, same input/query, rows returned |
| Schema and data model | Schema inventory/ERD, feature timestamps, dimension/fact/SCD2 if the corresponding v2 proof exists |

<a id="dung-don"></a>
## 7. Stop when idle, tear down the environment (laptop)

In `infra/aws/terraform`, after copying the receipts/logs/screenshots you need to keep to the laptop:

```bash
aws ec2 stop-instances --region ap-southeast-1 \
  --instance-ids "$(terraform output -raw instance_id)"
aws ec2 wait instance-stopped --region ap-southeast-1 \
  --instance-ids "$(terraform output -raw instance_id)"
# Next session (instance ID and key do not change; SSM needs time to come back online):
aws ec2 start-instances --region ap-southeast-1 \
  --instance-ids "$(terraform output -raw instance_id)"
# To tear down: read the destroy plan; only confirm after backing up the data you need to keep.
terraform plan -destroy
terraform destroy
```

Expected when idle: EC2 stopped, no compute charges; **EBS and S3 are still billed**.
The auto-assigned public IPv4 is released on stop; the configuration has no Elastic IP.
Destroy deletes the data EBS volume too; S3 buckets with remaining objects/versions block it because `force_destroy=false`.
If destroy reports a non-empty bucket: check your backups and every version/delete marker
before deciding to delete the data; do not just `aws s3 rm` and assume all versions are gone.
After tearing down, check the EC2/EBS/S3/Budgets Console and billing; for IAM/state errors, fix them
with the right profile/state; do not delete the state to skip resources.

<a id="chi-phi"></a>
## 8. Cost estimate

Use the [AWS Pricing Calculator](https://calculator.aws/) with Singapore, Linux
On-Demand, `t3.xlarge`, EBS gp3 **30 + 100 GiB**, public IPv4 per running hour,
S3 Standard by storage/versions/requests, and actual data transfer.
Check prices at [EC2](https://aws.amazon.com/ec2/pricing/on-demand/),
[EBS](https://aws.amazon.com/ebs/pricing/), [S3](https://aws.amazon.com/s3/pricing/)
and [IPv4](https://aws.amazon.com/vpc/pricing/) before applying.

**Budget estimate, not an AWS quote checked online:** assuming
compute $0.25/hour, IPv4 $0.005/hour, gp3 $0.10/GiB-month, S3 $0.025/GB-month.
Running 40 hours/month + 130 GiB of EBS kept all month + 20 GB of S3 ≈ **$23.70/month**;
leaving EC2 running 730 hours ≈ **$199.65/month** with the same assumptions. Add requests,
transfer, S3 versions, taxes and price changes; replace the assumed unit prices with the prices on the
AWS pages at the time you run. No network access was used while writing this document.
Credits only cover eligible charges and do not bring the cost to 0; the $30 budget is
an alert and does not stop the machine. There is no automatic stop schedule yet.

## Restarting EKS from 0 nodes

After scaling the node group back to 2, do the following in order before deploying:

1. **Substrate Valkey:** the pods get new IPs but the cluster still remembers the old ones ⇒ `cluster_state:fail`, `ate-api` not Ready, `kagent-controller` in `CrashLoopBackOff`. Fix it without data loss (the cluster has no password), then check `cluster_state:ok` with all 16384 slots; the other two pods recover on their own:
   ```bash
   K="kubectl --context platform-apps -n ate-system"
   IPS=$($K get pods -o wide --no-headers | awk '/^valkey-cluster/{print $6}')
   for p in 0 1 2 3 4 5; do for ip in $IPS; do $K exec valkey-cluster-$p -- valkey-cli cluster meet "$ip" 6379; done; done
   $K exec valkey-cluster-0 -- valkey-cli cluster info | head -3
   ```
2. **Node IPs change:** re-run Ansible with `-e feast_redis=<node IP>:30379 -e rag_pg=<node IP>:30432`, otherwise the EC2 `.env` still points to the old IP. A NodePort is reachable through any node.
3. **LLM:** `kubectl --context platform-apps -n llm scale deploy/qwen --replicas=1`, then wait for the rollout before running eval; without the LLM, eval scores 0.
4. **Jenkins:** use `python3 -m scripts.shared.setup_jenkins` instead of `docker compose up jenkins` (which lacks `DOCKER_GID` and the kubeconfig); run `scripts.shared.setup_vault` first to unseal Vault. The jobs clone `main` from the public GitHub repo (`CI_GIT_REMOTE` in `docker-compose.aws.yml`); job `ci` polls it every 5 minutes and runs the tests, the deploy jobs stay manual. Needs outbound HTTPS from EC2; nothing to copy by hand.
