variable "region" {
  default = "us-west-2"
}

variable "eks_instance_type" {
  default = "m5a.large"
}

variable "eks_instance_capacity" {
  default = 2
}

variable "postgres_instance_type" {
  default = "db.m5.xlarge"
}

locals {
  deployment_name = "mapbox-eks"
  cluster_name    = "mapbox-eks-cluster"
  cluster_version = "1.18"
}

provider "aws" {
  version = ">= 2.28.1"
  region  = var.region
}
