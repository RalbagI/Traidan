terraform {
  required_version = ">= 1.8"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  skip_credentials_validation = var.aws_skip_credentials_validation
  skip_metadata_api_check     = var.aws_skip_metadata_api_check
  skip_requesting_account_id  = var.aws_skip_requesting_account_id
}

locals {
  resource_name_prefix  = var.resource_name_prefix
  exchange_peer_enabled = var.exchange_vpc_id != ""
  exchange_peer_owner_id = (
    var.exchange_peer_owner_id == "" ? null : var.exchange_peer_owner_id
  )
  exchange_peer_region = (
    var.exchange_peer_region == "" ? null : var.exchange_peer_region
  )
  exchange_peer_cross_account = local.exchange_peer_owner_id != null
  exchange_peer_cross_region = (
    local.exchange_peer_region != null && local.exchange_peer_region != var.aws_region
  )
  exchange_peer_auto_accept = (
    local.exchange_peer_enabled
    && !local.exchange_peer_cross_account
    && !local.exchange_peer_cross_region
  )
  exchange_peer_routes = local.exchange_peer_enabled ? {
    for route in setproduct(
      sort(tolist(var.exchange_peer_route_table_ids)),
      sort(tolist(var.exchange_peer_cidr_blocks))
    ) :
    "${route[0]} ${route[1]}" => {
      route_table_id = route[0]
      cidr_block     = route[1]
    }
  } : {}
}

# ---------------------------------------------------------------------------
# Public dry-run WSS endpoint ingress -- exposes dry-run WSS /stream over TLS
# only. Operators provide CIDR allowlists for browser reviewers; metrics remain
# private and are intentionally not exposed by this security group.
# ---------------------------------------------------------------------------
resource "aws_security_group" "dry_run_public_wss" {
  name        = "${local.resource_name_prefix}-dry-run-public-wss"
  description = "dry-run WSS /stream over TLS only"
  vpc_id      = var.vpc_id

  dynamic "ingress" {
    for_each = var.dry_run_wss_allowed_cidr_blocks

    content {
      description = "dry-run WSS /stream over TLS"
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = [ingress.value]
    }
  }

  egress {
    description = "allow outbound dry-run market data and package updates"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "${local.resource_name_prefix}-dry-run-public-wss"
    Project = local.resource_name_prefix
  }
}

# ---------------------------------------------------------------------------
# Cluster Placement Group — forces instances onto shared physical spine,
# minimizing inter-node switch hops. See architecture.md §2.
#
# Trade-off: single-AZ placement sacrifices multi-AZ HA; OMS-level failover
# handles secondary routing instead of infrastructure replication.
# ---------------------------------------------------------------------------
resource "aws_placement_group" "execution" {
  name     = "${local.resource_name_prefix}-execution"
  strategy = "cluster"
}

# ---------------------------------------------------------------------------
# Cold storage — QuestDB historical exports land in S3 as snappy Parquet files
# partitioned by symbol/date for offline ML training.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "cold_storage" {
  bucket = var.cold_storage_bucket_name

  tags = {
    Name    = "${local.resource_name_prefix}-cold-storage"
    Project = local.resource_name_prefix
  }
}

resource "aws_s3_bucket_public_access_block" "cold_storage" {
  bucket = aws_s3_bucket.cold_storage.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "cold_storage" {
  bucket = aws_s3_bucket.cold_storage.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "cold_storage" {
  bucket = aws_s3_bucket.cold_storage.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "cold_storage" {
  bucket = aws_s3_bucket.cold_storage.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {
      prefix = ""
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_iam_role" "cold_storage_export" {
  name = "${local.resource_name_prefix}-cold-storage-export"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name    = "${local.resource_name_prefix}-cold-storage-export"
    Project = local.resource_name_prefix
  }
}

resource "aws_iam_role_policy" "cold_storage_export" {
  name = "${local.resource_name_prefix}-cold-storage-export"
  role = aws_iam_role.cold_storage_export.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListColdStorageBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.cold_storage.arn
      },
      {
        Sid    = "ReadWriteColdStorageObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = "${aws_s3_bucket.cold_storage.arn}/*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "cold_storage_export" {
  name = "${local.resource_name_prefix}-cold-storage-export"
  role = aws_iam_role.cold_storage_export.name
}

# ---------------------------------------------------------------------------
# Execution engine instance — bare-metal for zero hypervisor jitter.
# ---------------------------------------------------------------------------
resource "aws_instance" "execution_engine" {
  ami                  = var.ami_id
  instance_type        = "z1d.metal"
  iam_instance_profile = aws_iam_instance_profile.cold_storage_export.name
  placement_group      = aws_placement_group.execution.id
  subnet_id            = var.subnet_id
  key_name             = var.key_name
  vpc_security_group_ids = concat(
    sort(tolist(var.additional_execution_security_group_ids)),
    [aws_security_group.dry_run_public_wss.id],
  )

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
    iops        = 16000
    throughput  = 1000
  }

  tags = {
    Name    = "${local.resource_name_prefix}-execution"
    Project = local.resource_name_prefix
  }
}

# ---------------------------------------------------------------------------
# VPC Peering — bypasses public internet to exchange order gateways.
# Peer VPC IDs provided by exchange (where available).
# ---------------------------------------------------------------------------
resource "aws_vpc_peering_connection" "exchange_peer" {
  count = var.exchange_vpc_id == "" ? 0 : 1

  vpc_id        = var.vpc_id
  peer_vpc_id   = var.exchange_vpc_id
  peer_owner_id = local.exchange_peer_owner_id
  peer_region   = local.exchange_peer_region
  # auto_accept only works for same-account peering; cross-account exchange
  # peering must be accepted manually or managed from the peer account.
  # Cross-account or cross-region peering stays pending for accepter approval.
  auto_accept = local.exchange_peer_auto_accept

  tags = { Name = "${local.resource_name_prefix}-exchange-peer" }

  lifecycle {
    precondition {
      condition = (
        length(var.exchange_peer_route_table_ids) > 0
        && length(var.exchange_peer_cidr_blocks) > 0
      )
      error_message = "Set exchange_peer_route_table_ids and exchange_peer_cidr_blocks when exchange_vpc_id is set."
    }
  }
}

resource "aws_route" "exchange_peer" {
  for_each = local.exchange_peer_routes

  route_table_id            = each.value.route_table_id
  destination_cidr_block    = each.value.cidr_block
  vpc_peering_connection_id = aws_vpc_peering_connection.exchange_peer[0].id
}
