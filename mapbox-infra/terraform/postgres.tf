# resource "aws_db_instance" "postgres" {
#   allocated_storage      = 100
#   engine                 = "postgres"
#   engine_version         = "11.5"
#   identifier             = "${local.deployment_name}-postgres"
#   instance_class         = var.postgres_instance_type
#   password               = "password"
#   username               = "postgres"
#   vpc_security_group_ids = [aws_security_group.pg_sec_group.id]
#   db_subnet_group_name   = aws_db_subnet_group.default.name
#   name                   = "testdb"
#   skip_final_snapshot    = true

#   tags = {
#     CreatedBy      = "terraform"
#     DeploymentName = local.deployment_name
#   }
# }
