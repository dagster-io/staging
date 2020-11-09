output "region" {
  description = "AWS region"
  value       = var.region
}

output "eks_cluster_name" {
  description = "Kubernetes Cluster Name"
  value       = local.cluster_name
}

output "eks_connection_command" {
  description = "This command updates your kubeconfig to point to the eks cluster"
  value       = "aws eks --region ${var.region} update-kubeconfig --name ${local.cluster_name}"
}

output "postgres_address" {
  description = "Use this in your values.yaml"
  value       = aws_db_instance.postgres.address
}

output "postgres_port" {
  description = "Use this in your values.yaml"
  value       = aws_db_instance.postgres.port
}

output "postgres_name" {
  description = "Use this in your values.yaml"
  value       = aws_db_instance.postgres.name
}

output "postgres_username" {
  description = "Use this in your values.yaml"
  value       = aws_db_instance.postgres.username
}

output "postgres_password" {
  description = "Use this in your values.yaml"
  value       = aws_db_instance.postgres.password
}
