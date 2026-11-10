# Outputs are configuration only; check workload readiness with kubectl after apply.
output "cluster_name" { value = aws_eks_cluster.apps.name }
output "region" { value = var.region }
output "data_private_ip" { value = data.terraform_remote_state.data.outputs.data_private_ip }
output "image_registry" { value = data.terraform_remote_state.data.outputs.image_registry }
output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --region ${var.region} --name ${aws_eks_cluster.apps.name} --alias ${aws_eks_cluster.apps.name}"
}

output "admin_role_arn" { value = var.admin_role_arn }
