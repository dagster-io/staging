# pylint doesn't know about pytest fixtures
# pylint: disable=unused-argument

import base64
import os
from contextlib import contextmanager

import boto3
import docker
from dagster import execute_pipeline
from dagster.utils import merge_dicts
from dagster.utils.test.postgres_instance import postgres_instance_for_test
from dagster.utils.yaml_utils import merge_yamls
from dagster_test.test_project import (
    build_and_tag_test_image,
    get_test_project_recon_pipeline,
    test_project_docker_image,
    test_project_environments_path,
)

IS_BUILDKITE = os.getenv("BUILDKITE") is not None


@contextmanager
def celery_docker_postgres_instance(overrides=None):
    with postgres_instance_for_test(
        __file__, "test-postgres-db-celery-docker", overrides=overrides
    ) as instance:
        yield instance


def _get_or_build_local_image(docker_image):
    try:
        client = docker.from_env()
        client.images.get(docker_image)
        print(  # pylint: disable=print-call
            "Found existing image tagged {image}, skipping image build. To rebuild, first run: "
            "docker rmi {image}".format(image=docker_image)
        )
    except docker.errors.ImageNotFound:
        build_and_tag_test_image(docker_image)


def _get_registry_config():
    ecr_client = boto3.client("ecr", region_name="us-west-1")
    token = ecr_client.get_authorization_token()
    username, password = (
        base64.b64decode(token["authorizationData"][0]["authorizationToken"]).decode().split(":")
    )
    registry = token["authorizationData"][0]["proxyEndpoint"]

    return {
        "url": registry,
        "username": username,
        "password": password,
    }


def test_execute_celery_docker_image_on_executor_config():
    docker_image = test_project_docker_image()
    docker_config = {
        "image": docker_image,
        "env_vars": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",],
        "network": "container:test-postgres-db-celery-docker",
    }

    if IS_BUILDKITE:
        docker_config["registry"] = _get_registry_config()
    else:
        _get_or_build_local_image(docker_image)

    run_config = merge_dicts(
        merge_yamls(
            [
                os.path.join(test_project_environments_path(), "env.yaml"),
                os.path.join(test_project_environments_path(), "env_s3.yaml"),
            ]
        ),
        {
            "execution": {
                "celery-docker": {
                    "config": {
                        "docker": docker_config,
                        "config_source": {"task_always_eager": True},
                    }
                }
            },
        },
    )

    with celery_docker_postgres_instance() as instance:

        result = execute_pipeline(
            get_test_project_recon_pipeline("docker_celery_pipeline"),
            run_config=run_config,
            instance=instance,
        )
        assert result.success


def test_execute_celery_docker_image_on_pipeline_config():
    docker_image = test_project_docker_image()
    docker_config = {
        "env_vars": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",],
        "network": "container:test-postgres-db-celery-docker",
    }

    if IS_BUILDKITE:
        docker_config["registry"] = _get_registry_config()

    else:
        _get_or_build_local_image(docker_image)

    run_config = merge_dicts(
        merge_yamls(
            [
                os.path.join(test_project_environments_path(), "env.yaml"),
                os.path.join(test_project_environments_path(), "env_s3.yaml"),
            ]
        ),
        {
            "execution": {
                "celery-docker": {
                    "config": {
                        "docker": docker_config,
                        "config_source": {"task_always_eager": True},
                    }
                }
            },
        },
    )

    with celery_docker_postgres_instance() as instance:
        result = execute_pipeline(
            get_test_project_recon_pipeline("docker_celery_pipeline", docker_image),
            run_config=run_config,
            instance=instance,
        )
        assert result.success
