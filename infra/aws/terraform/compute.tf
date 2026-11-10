# One Ubuntu 24.04 EC2 with a separate data EBS; no runtime install via user_data (Ansible handles it).
# Query Canonical's public SSM parameter instead of searching by name: this account cannot list
# Canonical images via DescribeImages (0 results), but the SSM parameter still returns the latest AMI.
data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

resource "aws_key_pair" "operator" {
  key_name   = "${var.name_prefix}-data-platform"
  public_key = var.ssh_public_key
}

resource "aws_instance" "data_platform" {
  ami                         = data.aws_ssm_parameter.ubuntu.insecure_value # public AMI ID, not a secret: shown in the plan
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.vm.id]
  iam_instance_profile        = aws_iam_instance_profile.vm.name
  key_name                    = aws_key_pair.operator.key_name
  associate_public_ip_address = true # public IP instead of a NAT Gateway (~$32/month)

  # Only the t family has CPU credits; m6i skips this block.
  dynamic "credit_specification" {
    for_each = startswith(var.instance_type, "t") ? [1] : []
    content {
      cpu_credits = var.cpu_credits
    }
  }

  # IMDSv2 required; hop limit 2 so Docker containers (one extra network hop) can still get credentials.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.boot_disk_gb
    encrypted             = true
    delete_on_termination = true
  }

  tags = { Name = "${var.name_prefix}-data-platform" }

  # A newly released AMI must not replace the running machine. A stopped machine has no public IP ⇒ the provider reads
  # associate_public_ip_address=false ⇒ the plan wants to replace the machine (old one deleted on 30/10) ⇒ ignore this attribute.
  lifecycle {
    ignore_changes = [ami, associate_public_ip_address]
    precondition {
      condition     = startswith(var.availability_zone, "${var.region}")
      error_message = "availability_zone must belong to the selected region."
    }
  }
}

# Independent disk: replacing the EC2 keeps the data; `terraform destroy` still deletes it.
resource "aws_ebs_volume" "data" {
  availability_zone = var.availability_zone
  size              = var.data_disk_gb
  type              = "gp3"
  encrypted         = true
  tags              = { Name = "${var.name_prefix}-data-platform-data" }
}

# Nitro instances see /dev/sdf as nvme; Ansible finds it by volume ID, so it does not depend on this name.
resource "aws_volume_attachment" "data" {
  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.data.id
  instance_id = aws_instance.data_platform.id
}
