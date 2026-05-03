terraform {
  required_version = ">= 1.8"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "traderalbag-tf-state"
    key    = "prod/terraform.tfstate"
    region = "ap-northeast-1"
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# Cluster Placement Group — forces instances onto shared physical spine,
# minimizing inter-node switch hops. See architecture.md §2.
# ---------------------------------------------------------------------------
resource "aws_placement_group" "execution" {
  name     = "traderalbag-execution"
  strategy = "cluster"
}

# ---------------------------------------------------------------------------
# Execution engine instance — bare-metal for zero hypervisor jitter.
# ---------------------------------------------------------------------------
resource "aws_instance" "execution_engine" {
  ami                  = var.ami_id
  instance_type        = "z1d.metal"
  placement_group      = aws_placement_group.execution.id
  subnet_id            = var.subnet_id
  key_name             = var.key_name

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
    iops        = 16000
    throughput  = 1000
  }

  tags = {
    Name    = "traderalbag-execution"
    Project = "traderalbag"
  }
}

# ---------------------------------------------------------------------------
# VPC Peering — bypasses public internet to exchange order gateways.
# Peer VPC IDs provided by exchange (where available).
# ---------------------------------------------------------------------------
resource "aws_vpc_peering_connection" "exchange_peer" {
  vpc_id      = var.vpc_id
  peer_vpc_id = var.exchange_vpc_id
  auto_accept = true

  tags = { Name = "traderalbag-exchange-peer" }
}
