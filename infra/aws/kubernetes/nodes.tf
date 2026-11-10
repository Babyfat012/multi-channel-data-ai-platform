# The managed node group has no SSH key; image pulls use the node role, workloads use Pod Identity.
data "aws_iam_policy_document" "node_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${var.name_prefix}-eks-nodes"
  assume_role_policy = data.aws_iam_policy_document.node_trust.json
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "AmazonEKSWorkerNodePolicy", "AmazonEC2ContainerRegistryPullOnly", "AmazonEKS_CNI_Policy",
  ])
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/${each.key}"
}

resource "aws_launch_template" "node" {
  name_prefix = "${var.name_prefix}-eks-"
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size           = var.node_disk_gb
      volume_type           = "gp3"
      encrypted             = true
      delete_on_termination = true
    }
  }
}

resource "aws_eks_node_group" "apps" {
  cluster_name    = aws_eks_cluster.apps.name
  node_group_name = "${var.name_prefix}-apps"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids = var.single_az_workers ? [
    data.terraform_remote_state.data.outputs.kubernetes_subnet_ids[0]
  ] : data.terraform_remote_state.data.outputs.kubernetes_subnet_ids
  instance_types = var.node_instance_types
  ami_type       = "AL2023_x86_64_STANDARD"
  capacity_type  = "ON_DEMAND"
  scaling_config {
    desired_size = var.node_desired_size
    min_size     = var.node_min_size
    max_size     = var.node_max_size
  }
  launch_template {
    id      = aws_launch_template.node.id
    version = aws_launch_template.node.latest_version
  }
  update_config {
    max_unavailable = 1
    update_strategy = var.node_update_strategy
  }
  depends_on = [aws_iam_role_policy_attachment.node]
  lifecycle {
    precondition {
      condition     = var.node_max_size >= var.node_desired_size && var.node_desired_size >= var.node_min_size && var.node_max_size >= 1
      error_message = "Requires max >= desired >= min >= 0 and max >= 1."
    }
  }
}

resource "aws_eks_addon" "pod_identity" {
  cluster_name = aws_eks_cluster.apps.name
  addon_name   = "eks-pod-identity-agent"
  depends_on   = [aws_eks_node_group.apps]
}
