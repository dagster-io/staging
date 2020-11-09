module "eks" {
  source          = "terraform-aws-modules/eks/aws"
  cluster_name    = local.cluster_name
  cluster_version = local.cluster_version
  subnets         = module.vpc.private_subnets

  tags = {
    CreatedBy      = "terraform"
    DeploymentName = local.deployment_name
    ManagedBy      = "elementl"
  }

  vpc_id = module.vpc.vpc_id

  worker_groups = [
    {
      name                          = "worker-group"
      instance_type                 = var.eks_instance_type
      asg_desired_capacity          = var.eks_instance_capacity
      additional_security_group_ids = [aws_security_group.k8s_workers.id]
      tags = [{
        key                 = "ManagedBy"
        value               = "elementl"
        propagate_at_launch = true
      }]
    }
  ]

  write_kubeconfig = false

  map_roles = [
    {
      rolearn  = "arn:aws:iam::007292508084:role/TestMapboxClusterManagerRole"
      username = "TestMapboxClusterManagerRole"
      groups   = ["system:masters"]
    }
  ]
}

data "aws_eks_cluster" "cluster" {
  name = module.eks.cluster_id
}

data "aws_eks_cluster_auth" "cluster" {
  name = module.eks.cluster_id
}
