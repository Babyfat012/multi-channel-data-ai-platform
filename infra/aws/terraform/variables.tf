# Inputs for one Data Platform EC2; an instance allowlist prevents accidentally picking a size that burns all the credits.
variable "region" {
  type    = string
  default = "ap-southeast-1"
  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9]$", var.region))
    error_message = "Region must look like ap-southeast-1."
  }
}

variable "availability_zone" {
  description = "EBS and EC2 must share an AZ; EBS cannot attach across AZs."
  type        = string
  default     = "ap-southeast-1a"
  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9][a-z]$", var.availability_zone))
    error_message = "AZ must look like ap-southeast-1a."
  }
}

variable "name_prefix" {
  type    = string
  default = "platform"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,16}[a-z0-9]$", var.name_prefix))
    error_message = "Prefix must be 2–18 lowercase letters/digits/hyphens so bucket names are valid."
  }
}

variable "instance_type" {
  description = "t3.xlarge = 4 vCPU / 16 GiB, matching the limits in docker-compose.aws.yml."
  type        = string
  default     = "t3.xlarge"
  validation {
    condition     = contains(["t3.xlarge", "t3.2xlarge", "m6i.xlarge", "m6i.2xlarge", "c6i.2xlarge", "r6i.xlarge"], var.instance_type)
    error_message = "The overlay needs at least 16 GiB RAM: pick t3.xlarge/2xlarge, m6i.xlarge/2xlarge, c6i.2xlarge or r6i.xlarge."
  }
}

variable "cpu_credits" {
  description = "standard: throttles once CPU credits run out, instead of charging extra like unlimited."
  type        = string
  default     = "standard"
  validation {
    condition     = contains(["standard", "unlimited"], var.cpu_credits)
    error_message = "cpu_credits accepts only standard or unlimited."
  }
}

variable "ssh_public_key" {
  description = "Public key for SSH over SSM; port 22 stays closed. Must be set explicitly."
  type        = string
  nullable    = false
  validation {
    condition     = can(regex("^(ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp256) ", var.ssh_public_key))
    error_message = "Paste the contents of the .pub file (starting with ssh-ed25519, ssh-rsa or ecdsa-sha2-nistp256)."
  }
}

variable "boot_disk_gb" {
  type    = number
  default = 30
  validation {
    condition     = var.boot_disk_gb >= 20 && floor(var.boot_disk_gb) == var.boot_disk_gb
    error_message = "Boot disk must be an integer ≥20 GiB."
  }
}

variable "data_disk_gb" {
  description = "EBS is billed even while the EC2 is stopped; a larger size is a higher fixed cost."
  type        = number
  default     = 100
  validation {
    condition     = var.data_disk_gb >= 10 && floor(var.data_disk_gb) == var.data_disk_gb
    error_message = "Data disk must be an integer ≥10 GiB."
  }
}

variable "enable_budget" {
  type    = bool
  default = false
}

variable "budget_email" {
  description = "Unlike GCP, AWS does not email anyone automatically; set the recipient when enabling the budget."
  type        = string
  default     = null
}

variable "budget_amount_usd" {
  type    = number
  default = 30
  validation {
    condition     = var.budget_amount_usd > 0 && floor(var.budget_amount_usd) == var.budget_amount_usd
    error_message = "Budget USD must be a positive integer; it is an alert, not a spending cap."
  }
}

variable "tags" {
  type = map(string)
  default = {
    project     = "platform"
    environment = "learning"
  }
}

variable "noncurrent_retention_days" {
  description = "Delete noncurrent object versions after this many days; live Iceberg data is not deleted."
  type        = number
  default     = 30
  validation {
    condition     = var.noncurrent_retention_days >= 1 && floor(var.noncurrent_retention_days) == var.noncurrent_retention_days
    error_message = "Retention must be a positive integer number of days."
  }
}

variable "temp_retention_days" {
  description = "Deletes only objects under the tmp/ prefix; do not store authoritative data there."
  type        = number
  default     = 7
  validation {
    condition     = var.temp_retention_days >= 1 && floor(var.temp_retention_days) == var.temp_retention_days
    error_message = "Temporary retention must be a positive integer number of days."
  }
}

variable "force_destroy" {
  description = "By default, refuse to delete buckets that still hold data; enable only after checking and backing up."
  type        = bool
  default     = false
}

# Extensions are created only when the operator enables them in tfvars and applies the plan.
variable "enable_ecr" {
  type    = bool
  default = false
}

variable "enable_kubernetes_network" {
  type    = bool
  default = false
}

variable "secondary_availability_zone" {
  description = "Second AZ in the same region for the EKS subnets; required when the Kubernetes network is enabled."
  type        = string
  default     = null
}
