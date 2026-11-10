# Cloud preparation and deployment

The deployment target is AWS for the Data Platform, ML Platform and LLM Platform. The operator runs the AWS commands, Terraform plan/apply and Ansible directly. The repo's offline checks do not confirm that resources or workloads are running on AWS.

The demo configuration and budget record resource presets and the start/stop schedule. Recorded configuration: the EC2 Data Platform instance and two EKS `r6i.xlarge` workers, 12 vCPU in total; see the [AWS validation status](aws_validation.md).

## 1. Check the account before creating resources

You need AWS CLI v2 logged in with your profile, Terraform, the SSM plugin, Docker, kubectl, Helm, SSH/rsync and Ansible. Check these tools on the control machine before running anything. Install Ansible in a separate environment following the [official guide](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html).

```bash
# Replace PROFILE_NAME with a logged-in profile; never share access keys/passwords.
export AWS_PROFILE=PROFILE_NAME
export AWS_REGION=ap-southeast-2
python3 scripts/evidence/check_aws_readiness.py --profile "$AWS_PROFILE" --region "$AWS_REGION"
```

The script only calls read APIs: identity, account plan, Standard vCPU quota, EC2 specs/offerings and EKS versions. The private receipt in `out/cloud-preflight/` has `0600` permissions; a login error stops before the other service probes. `AccessDenied` is recorded as missing verification; permissions or profiles are not changed automatically.

Before planning, settle the account/role, region, two AZs, a budget that includes the part not covered by credits, node type/count, Kubernetes version and the admin egress IP. A quota is a limit and does not subtract resources already in use; an offering does not guarantee capacity. Prices, remaining credits and create permissions still need to be checked on your account. See [EKS pricing](https://aws.amazon.com/eks/pricing/), [EC2](https://aws.amazon.com/ec2/pricing/) and [ECR](https://aws.amazon.com/ecr/pricing/).

## 2. Data Platform, network and registry

The [Data Platform Terraform](../../infra/aws/terraform/README.md) root manages EC2/EBS/S3. The newer parts are all opt-in:

- `enable_ecr = true`: nine image repositories in the project namespace, immutable tags, scan-on-push, images not force-deleted on destroy.
- `enable_kubernetes_network = true`: adds a subnet in a second AZ. The two public subnets provide egress for the nodes; no NAT Gateway is created. Nodes have public IPs; there is no Internet ingress for applications yet.
- `secondary_availability_zone`: must differ from the first AZ and be in the same region.

```bash
cd infra/aws/terraform
cp terraform.tfvars.example terraform.tfvars
# Fill in the SSH PUBLIC key, region/AZ, instance and budget/email.
# Enable enable_ecr, enable_kubernetes_network and fill in secondary_availability_zone.
terraform init
terraform fmt -check
terraform validate
terraform plan -out data-platform.tfplan
# Read the plan; check the account/region and every create/replace/delete before continuing.
terraform apply data-platform.tfplan
terraform output
cd ../../..
```

Do not commit tfvars, state or plans. If you already have old state, read the migration guide in the Terraform README before applying. Set up the EC2 runtime with [Ansible](../../infra/aws/ansible/README.md); you still need to fill in the real inventory/variables and encrypt secrets with Ansible Vault.

## 3. Kubernetes for the ML Platform / LLM Platform

The [Kubernetes Terraform](../../infra/aws/kubernetes/README.md) root reads the outputs of the Data Platform state. This root creates a real EKS cluster and has its own cost when applied.

```bash
cd infra/aws/kubernetes
cp terraform.tfvars.example terraform.tfvars
# Fill in data_platform_state, a currently supported kubernetes_version,
# a permanent admin_role_arn, api_allowed_cidrs and the agreed node_instance_types.
terraform init
terraform fmt -check
terraform validate
terraform plan -out kubernetes.tfplan
# Review node count, disk, roles and endpoint before applying.
terraform apply kubernetes.tfplan
terraform output -raw kubeconfig_command
# Run the update-kubeconfig command just printed; the default alias is platform-apps.
cd ../../..
export KCTX=platform-apps
kubectl --context "$KCTX" get nodes
kubectl --context "$KCTX" -n kube-system get pods
kubectl --context "$KCTX" -n kube-system rollout status daemonset/eks-pod-identity-agent --timeout=180s
kubectl --context "$KCTX" -n kube-system rollout status deployment/ebs-csi-controller --timeout=180s
```

The cluster API always has a private endpoint; the public endpoint is limited to an admin CIDR of `/24` or narrower. Admin access is bound to a specific IAM role and is not granted automatically to every principal that creates the cluster. The AWS profile used for kubectl must correspond to that role; if you use a different profile, configure an allowed assume-role before fetching a token. Workloads get credentials through Pod Identity; nodes have no S3 data policy.

Check the existing storage classes before choosing a default. If another class is already the default, set the old class's annotation to `false`, then apply the repo's gp3 class:

```bash
kubectl --context "$KCTX" get storageclass
# Only if gp2 is currently the default:
# kubectl --context "$KCTX" annotate storageclass gp2 storageclass.kubernetes.io/is-default-class=false --overwrite
kubectl --context "$KCTX" apply -f infra/k8s/storage-aws.yaml
```

`gp3` uses EBS CSI and `WaitForFirstConsumer`; a PVC only binds once a pod uses it. PVs use `Retain`: deleting the workload/PVC/cluster does not guarantee that all EBS volumes are cleaned up. Keep both data and cost in mind when tearing down the environment.

## 4. Publish images and choose the cloud context

```bash
export REGISTRY=$(terraform -chdir=infra/aws/terraform output -raw image_registry)
export ECR_HOST=${REGISTRY%%/*}
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_HOST"
export TAG="$(git rev-parse HEAD)-BUILD_ID"  # replace BUILD_ID for each new build
# Example: build/push the Feature API; other images use the same helper and namespace.
docker build -f docker/Dockerfile.serving -t "platform-feature-api:$TAG" .
scripts/run/push_images.sh "platform-feature-api:$TAG"
```

ECR tags are immutable: do not overwrite a published tag. Record the commit SHA, build ID and digest when collecting evidence. The deploy scripts take `KCTX`; the `setup_kind_*` bootstrap scripts are still for local use and do not run against EKS. Jenkins has registry/context/kubeconfig credential parameters, but the Compose pipelines for RAG/stream still need their own AWS configuration before running end to end.

## 5. Feature API once its dependencies are ready

Only do this once Redis, pgvector, the Feast registry on S3 and KServe have been deployed/checked. This is the next integration check, not an automatic consequence of Terraform apply.

```bash
export PLATFORM_STORAGE=s3
export KCTX=platform-apps
export LAKEHOUSE_BUCKET=$(terraform -chdir=infra/aws/terraform output -raw lakehouse_bucket)
# POSTGRES_USER / POSTGRES_PASSWORD come from the environment's secret source, never written to Git.
SKIP_BUILD_PUSH=1 bash scripts/setup/deploy_feature_api.sh "$TAG"
kubectl --context "$KCTX" -n feature-store rollout status deployment/feature-api --timeout=180s
```

The AWS chart uses ClusterIP and the `feature-api` ServiceAccount in the `feature-store` namespace, matching Pod Identity. The Secret contains no static MinIO/AWS keys. Helm `--atomic` rolls back if the pods do not become Ready. The gateway/ingress is deployed separately; `setup_ingress.sh` needs `INGRESS_DOMAIN` in a cloud context and does not infer the IP from the kind container. The internal CA in the script does not replace a real ACME/domain configuration.

## 6. Gate before moving on to running pipelines

- EC2 is reachable via SSM; the Data Platform runtime is healthy; the S3/Iceberg smoke test passes.
- ECR push works and nodes pull the right digest; nodes are Ready; Pod Identity/EBS CSI work; the PVC smoke test checks read/write.
- The EC2 ↔ EKS connection is tested for real. Terraform only opens the EC2 ports allowed from the cluster SG; the Data Platform Compose stack currently binds to loopback. Configure private listeners and service endpoints before integration; do not expose UIs/databases to the Internet.
- MLflow/KFP/KServe, Redis/pgvector, LLM/agents, Vault, ingress and observability on the cloud still need their workloads deployed once the base infrastructure passes. The training scripts still have a MinIO credential branch; they are not considered cloud-ready just because they accept `KCTX`.
- Collect real receipts into `out/`; only move checked ones into [evidence](../data-platform/evidence.md). Do not update the status to “runs on the cloud” based on a schema check or a Helm render.

References: [EKS networking](https://docs.aws.amazon.com/eks/latest/best-practices/subnets.html), [node IAM role](https://docs.aws.amazon.com/eks/latest/userguide/create-node-role.html), [Pod Identity trust](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-role.html), [EBS CSI](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html).

[Documentation index](../README.md)
