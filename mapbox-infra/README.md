# Testing for Mapbox

Testing with this policy/role in `elementl-dev`:

TestMapboxIAMPolicy
TestMapboxIAMRole
TestMapboxClusterManagerPolicy
TestMapboxClusterManagerRole

Need Mapbox to create:
`s3://elementl-tf-mapbox-dev-test` (see `versions.tf`)

## Requirements

Env variables

- Set AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID
- Set INTERNAL_REPO to the location of the `internal` repo
- Set DAGSTER_REPO to the location of the `dagster` repo

Packages

- Install terraform: `brew install terraform`
- Install wget: `brew install wget`
- Install kubectl: https://kubernetes.io/docs/tasks/tools/install-kubectl/
- Install helm: https://helm.sh/docs/intro/install/
- Install `jq`, which is used in the bash scripts.
    - For macos, use `brew install jq`.

## Terraform

The initialization of the EKS cluster resources requires that you use your AWS IAM user to assume an IAM role that exists in another AWS account: `TestMapboxIAMRole`.

In the folder `terraform/`, run the following commands:

```sh
./assume_role_terraform.sh init
./assume_role_terraform.sh plan
./assume_role_terraform.sh apply
```

If something goes wrong with a 403 Forbidden / AccessDenied error, try this:

```sh
# Enable verbose output for terraform.
export TF_LOG=TRACE

# Retry the Terraform command.
./assume_role_terraform.sh apply

# Find the encoded error message in the output and copy it for the next line.
./decode_sts.sh <encoded error message>
```

Based on the specific AWS command that was forbidden (for example, `CreateVpc`), adjust the IAM privileges of `TestMapboxIAMPolicy`. Then, try to `apply` again.

## EKS Cluster Management (`kubectl` and `helm`)

The maintenance of the EKS cluster requires that you use your AWS IAM user to assume an IAM role that exists in another AWS account: `TestMapboxClusterManagerRole`.

This AWS IAM role has been added to the aws-auth configmap, and should be able to run `kubectl` and `helm` commands on your EKS cluster.

This AWS IAM role __has not__ been granted any privileges for manaing AWS resources, such as EC2, RDS, EKS, etc.

__Configure your local `kubectl`__

In the folder `helm/`, run the following commands:

```bash
./assume_role_eks.sh aws eks update-kubeconfig --name mapbox-eks-cluster --role-arn arn:aws:iam::007292508084:role/TestMapboxClusterManagerRole

# Check that kubectl is configured correctly.
./assume_role_eks.sh kubectl cluster-info
```
