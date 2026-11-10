# The cluster API is private for nodes; the public endpoint accepts only the declared admin CIDR.
data "aws_iam_policy_document" "cluster_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name               = "${var.name_prefix}-eks-control-plane"
  assume_role_policy = data.aws_iam_policy_document.cluster_trust.json
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "apps" {
  name     = "${var.name_prefix}-apps"
  role_arn = aws_iam_role.cluster.arn
  version  = var.kubernetes_version
  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = false
  }
  vpc_config {
    subnet_ids              = data.terraform_remote_state.data.outputs.kubernetes_subnet_ids
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = var.api_allowed_cidrs
  }
  enabled_cluster_log_types = ["api", "audit", "authenticator"]
  depends_on                = [aws_iam_role_policy_attachment.cluster]
  lifecycle {
    precondition {
      condition = (
        data.terraform_remote_state.data.outputs.region == var.region &&
        length(data.terraform_remote_state.data.outputs.kubernetes_subnet_ids) >= 2
      )
      error_message = "The Data Platform root must be in the same region and have applied enable_kubernetes_network with two subnets in different AZs."
    }
  }
}

resource "aws_eks_access_entry" "admin" {
  cluster_name  = aws_eks_cluster.apps.name
  principal_arn = var.admin_role_arn
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "admin" {
  cluster_name  = aws_eks_cluster.apps.name
  principal_arn = aws_eks_access_entry.admin.principal_arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
  access_scope { type = "cluster" }
}

resource "aws_vpc_security_group_ingress_rule" "data_services" {
  for_each                     = { for port in var.data_service_ports : tostring(port) => port }
  security_group_id            = data.terraform_remote_state.data.outputs.data_security_group_id
  referenced_security_group_id = aws_eks_cluster.apps.vpc_config[0].cluster_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = each.value
  to_port                      = each.value
  description                  = "EKS workloads to Data Platform private endpoint"
}

# Reverse direction: Airflow on EC2 materializes Feast into Redis NodePort 30379 (infra/k8s/feature-store/redis.yaml).
# Opens exactly one port, only from the Data Platform EC2 SG; nodes use the cluster SG because the launch template attaches no SG of its own.
resource "aws_vpc_security_group_ingress_rule" "data_to_redis" {
  security_group_id            = aws_eks_cluster.apps.vpc_config[0].cluster_security_group_id
  referenced_security_group_id = data.terraform_remote_state.data.outputs.data_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 30379
  to_port                      = 30379
  description                  = "Data Platform Airflow to Feast Redis NodePort"
}

# Wave 4: Airflow on EC2 (ml_rag_ingest) writes embeddings to pgvector NodePort 30432 (infra/k8s/rag/pgvector.yaml).
resource "aws_vpc_security_group_ingress_rule" "data_to_pgvector" {
  security_group_id            = aws_eks_cluster.apps.vpc_config[0].cluster_security_group_id
  referenced_security_group_id = data.terraform_remote_state.data.outputs.data_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 30432
  to_port                      = 30432
  description                  = "Data Platform Airflow to RAG pgvector NodePort"
}

# Jenkins on EC2 (wave 5): the EC2 role enters the cluster via the private endpoint with group `platform-ci` and NO AWS access
# policy. Real permissions live in infra/k8s/ci/rbac.yaml: full access only inside a few CI deploy namespaces (kagent's Agent/RemoteMCPServer
# are CRDs, which the AWS Edit policy does not cover).
data "aws_iam_role" "data_vm" {
  name = "${var.name_prefix}-data-platform-vm"
}

resource "aws_eks_access_entry" "ci" {
  cluster_name      = aws_eks_cluster.apps.name
  principal_arn     = data.aws_iam_role.data_vm.arn
  kubernetes_groups = ["platform-ci"]
  type              = "STANDARD"
}

resource "aws_vpc_security_group_ingress_rule" "data_to_api" {
  security_group_id            = aws_eks_cluster.apps.vpc_config[0].cluster_security_group_id
  referenced_security_group_id = data.terraform_remote_state.data.outputs.data_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
  description                  = "Data Platform Jenkins to EKS private API endpoint"
}
