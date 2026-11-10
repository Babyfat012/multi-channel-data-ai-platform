# Declares Terraform and pins the AWS provider; state stays local until the operator chooses to move it.
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
  # Common tags for every resource, replacing labels on GCP.
  default_tags {
    tags = var.tags
  }
}
