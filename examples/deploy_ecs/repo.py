import boto3
from dagster import pipeline, repository, solid


@solid
def whoami():
    return boto3.client("sts").get_caller_identity()["Arn"]


@pipeline
def my_pipeline():
    whoami()


@repository
def deploy_docker_repository():
    return [my_pipeline]
