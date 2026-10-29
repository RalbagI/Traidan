output "execution_instance_id" {
  description = "ID of the bare-metal execution engine instance."
  value       = aws_instance.execution_engine.id
}

output "execution_private_ip" {
  description = "Private IP address of the execution engine instance."
  value       = aws_instance.execution_engine.private_ip
}

output "execution_public_ip" {
  description = "Public IP address of the execution engine instance."
  value       = aws_instance.execution_engine.public_ip
}

output "dry_run_public_wss_security_group_id" {
  description = "Security group that exposes only the dry-run WSS endpoint on tcp/443."
  value       = aws_security_group.dry_run_public_wss.id
}

output "placement_group_name" {
  description = "Cluster placement group assigned to execution instances."
  value       = aws_placement_group.execution.name
}

output "cold_storage_bucket_name" {
  description = "S3 bucket receiving QuestDB snappy Parquet cold-storage exports."
  value       = aws_s3_bucket.cold_storage.bucket
}

output "cold_storage_export_role_arn" {
  description = "IAM role ARN used by the execution host to write cold-storage exports."
  value       = aws_iam_role.cold_storage_export.arn
}

output "exchange_peering_connection_id" {
  description = "VPC peering connection ID when exchange peering is enabled."
  sensitive   = true
  value       = try(aws_vpc_peering_connection.exchange_peer[0].id, null)
}

output "exchange_peer_route_ids" {
  description = "Route IDs propagated from execution route tables to exchange peer CIDRs."
  sensitive   = true
  value = {
    for route_key, route in aws_route.exchange_peer : route_key => route.id
  }
}
