# module "eks" {
#   source          = "terraform-aws-modules/eks/aws"
#   cluster_name    = local.cluster_name
#   cluster_version = local.cluster_version
#   subnets         = module.vpc.private_subnets

#   tags = {
#     CreatedBy      = "terraform"
#     DeploymentName = local.deployment_name
#   }

#   vpc_id = module.vpc.vpc_id

#   worker_groups = [
#     {
#       name                          = "worker-group"
#       instance_type                 = var.eks_instance_type
#       asg_desired_capacity          = var.eks_instance_capacity
#       additional_security_group_ids = [aws_security_group.k8s_workers.id]
#     }
#   ]

#   write_kubeconfig = false
# }

# data "aws_eks_cluster" "cluster" {
#   name = module.eks.cluster_id
# }

# data "aws_eks_cluster_auth" "cluster" {
#   name = module.eks.cluster_id
# }
