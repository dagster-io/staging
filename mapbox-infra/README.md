# Testing for Mapbox

Testing with this policy/role in `elementl-dev`:

TestMapboxIAMPolicy
TestMapboxIAMRole

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

In the terraform folder, run:

```sh
../assume_role_terraform.sh init
../assume_role_terraform.sh plan
../assume_role_terraform.sh apply
```

If something goes wrong w/ a 403 forbidden, try this:

```sh
export TF_LOG=TRACE
../assume_role_terraform.sh plan
# find encoded error message in output, copy to clipboard
../decode_sts.sh
```
