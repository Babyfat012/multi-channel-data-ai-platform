# Inputs must be settled against the real account/quota/budget before planning.
variable "region" {
  type    = string
  default = "ap-southeast-1"
}

variable "name_prefix" {
  type    = string
  default = "platform"
}

variable "data_platform_state" {
  description = "Local state path after applying the ../terraform root with the two-AZ network enabled. Do not commit state."
  type        = string
}

variable "kubernetes_version" {
  description = "Pick an EKS version AWS supports in the deployment region; do not pick it from the local kind version."
  type        = string
}

variable "admin_role_arn" {
  description = "Long-lived IAM role used to administer EKS; not an STS assumed-role session ARN."
  type        = string
  validation {
    condition     = can(regex("^arn:aws:iam::[0-9]{12}:role/.+", var.admin_role_arn))
    error_message = "admin_role_arn must be an IAM role ARN."
  }
}

variable "api_allowed_cidrs" {
  description = "Egress IP of the admin/CI machine, e.g. IPv4/32; the private endpoint is always on for nodes."
  type        = list(string)
  validation {
    condition = length(var.api_allowed_cidrs) > 0 && alltrue([
      for c in var.api_allowed_cidrs : can(cidrnetmask(c)) && try(tonumber(split("/", c)[1]) >= 24, false)
    ])
    error_message = "Requires a specific IPv4 CIDR of /24 or narrower; 0.0.0.0/0 is not allowed."
  }
}

variable "node_instance_types" {
  description = "x86_64 instance types of the same resource size; check quota and real prices before filling in."
  type        = list(string)
  validation {
    condition     = length(var.node_instance_types) > 0
    error_message = "Pick at least one node type."
  }
}

variable "node_desired_size" {
  type    = number
  default = 1
  validation {
    condition     = var.node_desired_size >= 0 && floor(var.node_desired_size) == var.node_desired_size
    error_message = "node_desired_size must be an integer >= 0; 0 pauses workloads."
  }
}

variable "node_min_size" {
  type    = number
  default = 1
  validation {
    condition     = var.node_min_size >= 0 && floor(var.node_min_size) == var.node_min_size
    error_message = "node_min_size must be an integer >= 0."
  }
}

variable "node_update_strategy" {
  description = "DEFAULT creates the replacement node first; MINIMAL lowers peak CPU quota but may interrupt workloads."
  type        = string
  default     = "DEFAULT"
  validation {
    condition     = contains(["DEFAULT", "MINIMAL"], var.node_update_strategy)
    error_message = "Pick DEFAULT or MINIMAL."
  }
}

variable "single_az_workers" {
  description = "Budget single worker: pin the AZ so EBS PVCs stay usable after scale-to-zero/resume. The control plane still spans two AZs."
  type        = bool
  default     = false
}

variable "node_max_size" {
  type    = number
  default = 2
}

variable "node_disk_gb" {
  type    = number
  default = 80
}

variable "data_service_ports" {
  description = "Only these EC2 ports are opened from the cluster SG. Default: PostgreSQL and Lakekeeper only."
  type        = set(number)
  default     = [5432, 8181]
  validation {
    condition     = alltrue([for p in var.data_service_ports : contains([5432, 8181, 5050, 8200, 9100, 9187], p)])
    error_message = "Only PostgreSQL, Lakekeeper, MLflow, Vault or exporters (9100/9187) are allowed; no public UI/SSH."
  }
}
