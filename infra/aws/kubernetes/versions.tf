# Separate Terraform root for EKS; applying only the Data Platform root does not provision a cluster.
terraform {
  required_version = ">= 1.6.0, < 2.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = var.name_prefix, component = "ml-llm-platform" }
  }
}

data "terraform_remote_state" "data" {
  backend = "local"
  config  = { path = var.data_platform_state }
}
