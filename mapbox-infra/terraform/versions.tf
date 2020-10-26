
terraform {
  required_version = ">= 0.12"

  backend "s3" {
    bucket = "elementl-tf-mapbox-dev-test"
    key    = "dev/mapbox-infra"
    region = "us-west-2"
  }
}

provider "random" {
  version = "~> 2.1"
}

provider "local" {
  version = "~> 1.2"
}

provider "null" {
  version = "~> 2.1"
}

provider "template" {
  version = "~> 2.1"
}
