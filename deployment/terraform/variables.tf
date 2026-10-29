variable "aws_region" {
  description = "Target AWS region for the dry-run execution host."
  type        = string
  default     = "us-east-1"

  validation {
    condition = contains([
      "ap-northeast-1",
      "us-east-1",
      "eu-west-1",
      "ap-southeast-1",
    ], var.aws_region)
    error_message = "aws_region must match a supported dry-run execution region."
  }
}

variable "resource_name_prefix" {
  description = "Prefix for Terraform-managed AWS resource names and project tags."
  type        = string
  default     = "traidan"

  validation {
    condition     = can(regex("^[a-z0-9-]{1,32}$", var.resource_name_prefix))
    error_message = "resource_name_prefix must be 1-32 lowercase letters, digits, or hyphens."
  }
}

variable "cold_storage_bucket_name" {
  description = "S3 bucket for QuestDB Parquet cold-storage exports."
  type        = string
  default     = "traidan-data"

  validation {
    condition = (
      can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.cold_storage_bucket_name))
      && !can(regex("\\.\\.", var.cold_storage_bucket_name))
      && !can(regex("^-", var.cold_storage_bucket_name))
      && !can(regex("-$", var.cold_storage_bucket_name))
    )
    error_message = "cold_storage_bucket_name must be a valid S3 bucket name."
  }
}

variable "ami_id" {
  description = "Ubuntu 24.04 LTS bare-metal AMI ID for target region."
  type        = string
}

variable "subnet_id" {
  description = "Subnet within the Cluster Placement Group AZ."
  type        = string
}

variable "key_name" {
  description = "EC2 SSH key pair name."
  type        = string
}

variable "vpc_id" {
  description = "VPC ID hosting the execution engine."
  type        = string
}

variable "additional_execution_security_group_ids" {
  description = "Existing security groups to attach to the execution instance for SSH, SSM, or private operations."
  type        = set(string)
  default     = []
}

variable "dry_run_wss_allowed_cidr_blocks" {
  description = "Operator or reviewer IPv4 CIDR blocks allowed to reach the public dry-run WSS endpoint on tcp/443."
  type        = set(string)
  default     = []

  validation {
    condition = alltrue([
      for cidr_block in var.dry_run_wss_allowed_cidr_blocks :
      cidr_block != "0.0.0.0/0"
      && try(tonumber(split("/", cidr_block)[1]), -1) >= 8
      && try(tonumber(split("/", cidr_block)[1]), -1) <= 32
      && can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/[0-9]{1,2}$", cidr_block))
      && can(cidrhost(cidr_block, 0))
    ])
    error_message = "dry_run_wss_allowed_cidr_blocks must contain valid IPv4 CIDR blocks between /8 and /32 and must not include 0.0.0.0/0."
  }
}

variable "exchange_vpc_id" {
  description = "Peer VPC ID of the exchange order gateway (where available)."
  type        = string
  default     = ""
}

variable "exchange_peer_owner_id" {
  description = "AWS account ID that owns the peer VPC. Leave blank for same-account peering."
  type        = string
  default     = ""

  validation {
    condition     = var.exchange_peer_owner_id == "" || can(regex("^[0-9]{12}$", var.exchange_peer_owner_id))
    error_message = "exchange_peer_owner_id must be blank or a 12-digit AWS account ID."
  }
}

variable "exchange_peer_region" {
  description = "AWS region of the peer VPC. Leave blank for same-region peering."
  type        = string
  default     = ""

  validation {
    condition = var.exchange_peer_region == "" || contains([
      "ap-northeast-1",
      "us-east-1",
      "eu-west-1",
      "ap-southeast-1",
    ], var.exchange_peer_region)
    error_message = "exchange_peer_region must be blank or a supported exchange colocation region."
  }
}

variable "exchange_peer_route_table_ids" {
  description = "Execution VPC route table IDs that should route exchange CIDRs over VPC peering."
  type        = set(string)
  default     = []
}

variable "exchange_peer_cidr_blocks" {
  description = "IPv4 CIDR blocks advertised by the peered exchange gateway VPC."
  type        = set(string)
  default     = []

  validation {
    condition = alltrue([
      for cidr_block in var.exchange_peer_cidr_blocks :
      cidr_block != "0.0.0.0/0"
      && try(tonumber(split("/", cidr_block)[1]), -1) >= 16
      && try(tonumber(split("/", cidr_block)[1]), -1) <= 32
      && can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/[0-9]{1,2}$", cidr_block))
      && can(cidrhost(cidr_block, 0))
    ])
    error_message = "exchange_peer_cidr_blocks must contain valid IPv4 CIDR blocks between /16 and /32, excluding 0.0.0.0/0."
  }
}

variable "aws_skip_credentials_validation" {
  description = "Skip AWS credential validation; used only by CI mock plans."
  type        = bool
  default     = false
}

variable "aws_skip_metadata_api_check" {
  description = "Skip AWS metadata API checks; used only by CI mock plans."
  type        = bool
  default     = false
}

variable "aws_skip_requesting_account_id" {
  description = "Skip AWS account ID lookup; used only by CI mock plans."
  type        = bool
  default     = false
}
