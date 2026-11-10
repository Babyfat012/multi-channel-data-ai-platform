# Hand-off interface for Ansible, plus commands for the operator to open sessions/UIs over SSM.
output "raw_bucket" {
  description = "Raw bucket name (without s3://)."
  value       = aws_s3_bucket.data["raw"].id
}

output "lakehouse_bucket" {
  description = "Iceberg warehouse bucket name (without s3://)."
  value       = aws_s3_bucket.data["lakehouse"].id
}

output "raw_bucket_uri" {
  value = "s3://${aws_s3_bucket.data["raw"].id}"
}

output "lakehouse_bucket_uri" {
  value = "s3://${aws_s3_bucket.data["lakehouse"].id}"
}

output "region" {
  value = var.region
}

output "vpc_id" {
  value = aws_vpc.data_platform.id
}

output "kubernetes_subnet_ids" {
  value = var.enable_kubernetes_network ? concat(
    [aws_subnet.public.id], aws_subnet.kubernetes_secondary[*].id
  ) : []
}

output "data_security_group_id" {
  value = aws_security_group.vm.id
}

output "data_private_ip" {
  value = aws_instance.data_platform.private_ip
}

output "ecr_repositories" {
  value = { for name, repo in aws_ecr_repository.application : name => repo.repository_url }
}

output "image_registry" {
  description = "Prefix for scripts/run/push_images.sh; includes the registry account and project namespace."
  value = var.enable_ecr ? trimsuffix(
    aws_ecr_repository.application["platform-feature-api"].repository_url, "/platform-feature-api"
  ) : null
}

output "instance_id" {
  description = "Use as ansible_host in the inventory (SSH goes through SSM by ID)."
  value       = aws_instance.data_platform.id
}

output "data_volume_id" {
  description = "Ansible finds the disk at /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_<id without the ->."
  value       = aws_ebs_volume.data.id
}

output "iam_role_name" {
  value = aws_iam_role.vm.name
}

output "ssm_command" {
  value = "aws ssm start-session --target ${aws_instance.data_platform.id} --region ${var.region}"
}

output "port_forward_command" {
  description = "SSH tunnel over SSM; uses the same key path as the sample inventory (change -i if the key lives elsewhere)."
  value       = "ssh -N -i ~/.ssh/platform_aws -o ExitOnForwardFailure=yes -o ProxyCommand='aws ssm start-session --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p --region ${var.region}' -L 127.0.0.1:8080:127.0.0.1:8080 -L 127.0.0.1:8081:127.0.0.1:8081 -L 127.0.0.1:8181:127.0.0.1:8181 -L 127.0.0.1:9002:127.0.0.1:9002 -L 127.0.0.1:8089:127.0.0.1:8089 -L 127.0.0.1:8082:127.0.0.1:8082 ubuntu@${aws_instance.data_platform.id}"
}
