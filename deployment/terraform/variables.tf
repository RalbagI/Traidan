variable "aws_region" {
  description = "Target AWS region — must match exchange matching engine location."
  type        = string
  default     = "ap-northeast-1"  # Tokyo — primary Binance region
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

variable "exchange_vpc_id" {
  description = "Peer VPC ID of the exchange order gateway (where available)."
  type        = string
  default     = ""
}
