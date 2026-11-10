# Dedicated VPC: outbound Internet access to pull images, no inbound ports (SSH goes through SSM).
resource "aws_vpc" "data_platform" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${var.name_prefix}-data-platform-vpc" }
}

# First /24 for milestone 1; milestone 2 (EKS) adds a subnet in another AZ within the same /16.
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.data_platform.id
  cidr_block              = "10.42.0.0/24"
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = var.enable_kubernetes_network
  tags = merge(
    { Name = "${var.name_prefix}-data-platform-public" },
    var.enable_kubernetes_network ? { "kubernetes.io/role/elb" = "1" } : {}
  )
}

# EKS needs subnets in two AZs. Nodes have public IPs for egress, no public SSH rule.
resource "aws_subnet" "kubernetes_secondary" {
  count                   = var.enable_kubernetes_network ? 1 : 0
  vpc_id                  = aws_vpc.data_platform.id
  cidr_block              = "10.42.1.0/24"
  availability_zone       = var.secondary_availability_zone
  map_public_ip_on_launch = true
  tags = {
    Name                     = "${var.name_prefix}-kubernetes-public-secondary"
    "kubernetes.io/role/elb" = "1"
  }

  lifecycle {
    precondition {
      condition = var.secondary_availability_zone == null ? false : (
        startswith(var.secondary_availability_zone, var.region) &&
        var.secondary_availability_zone != var.availability_zone
      )
      error_message = "Kubernetes requires a secondary_availability_zone different from the first AZ and in the same region."
    }
  }
}

resource "aws_route_table_association" "kubernetes_secondary" {
  count          = var.enable_kubernetes_network ? 1 : 0
  subnet_id      = aws_subnet.kubernetes_secondary[0].id
  route_table_id = aws_route_table.public.id
}

# AWS is closed by default: without a gateway + route the VM cannot reach the Internet.
resource "aws_internet_gateway" "data_platform" {
  vpc_id = aws_vpc.data_platform.id
  tags   = { Name = "${var.name_prefix}-data-platform-igw" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.data_platform.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.data_platform.id
  }
  tags = { Name = "${var.name_prefix}-data-platform-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# No ingress: the SSM agent calls out to AWS, so port 22 need not be open.
resource "aws_security_group" "vm" {
  name        = "${var.name_prefix}-data-platform-vm"
  description = "Data Platform EC2 instance: no ingress, egress for SSM, image pull and S3"
  vpc_id      = aws_vpc.data_platform.id
  egress {
    description = "All egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
