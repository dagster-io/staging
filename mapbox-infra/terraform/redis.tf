resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "${local.deployment_name}-redis"
  engine               = "redis"
  node_type            = var.redis_instance_type
  num_cache_nodes      = 1
  parameter_group_name = "default.redis3.2"
  engine_version       = "3.2.10"
  port                 = 6379
  security_group_ids   = [aws_security_group.redis_sec_group.id]
  subnet_group_name    = aws_elasticache_subnet_group.default.name

  tags = {
    CreatedBy      = "terraform"
    DeploymentName = local.deployment_name
    ManagedBy      = "elementl"
  }
}
