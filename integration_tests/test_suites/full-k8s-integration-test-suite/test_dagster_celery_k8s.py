import os

import docker
import pytest
from dagster_celery_k8s.launcher import CeleryK8sRunLauncher
from dagster_k8s_test_infra.cluster import define_cluster_provider_fixture
from dagster_test.test_project import (
    build_and_tag_test_image,
    test_project_docker_image,
    test_project_environments_path,
)
from test_integration_base import TestK8sIntegration

from dagster.utils.merger import merge_dicts
from dagster.utils.yaml_utils import merge_yamls

IS_BUILDKITE = os.getenv("BUILDKITE") is not None


@pytest.fixture(scope="session")
def dagster_docker_image():
    docker_image = test_project_docker_image()

    if not IS_BUILDKITE:
        try:
            client = docker.from_env()
            client.images.get(docker_image)
            print(  # pylint: disable=print-call
                "Found existing image tagged {image}, skipping image build. To rebuild, first run: "
                "docker rmi {image}".format(image=docker_image)
            )
        except docker.errors.ImageNotFound:
            build_and_tag_test_image(docker_image)

    return docker_image


def get_celery_engine_config(
    dagster_docker_image, job_namespace
):  # pylint: disable=redefined-outer-name
    return {
        "execution": {
            "celery-k8s": {
                "config": {
                    "job_image": dagster_docker_image,
                    "job_namespace": job_namespace,
                    "image_pull_policy": "Always",
                    "env_config_maps": ["dagster-pipeline-env"],
                }
            }
        },
    }


cluster_provider = define_cluster_provider_fixture()


class TestCeleryDagsterK8sIntegration(TestK8sIntegration):
    __test__ = True

    @pytest.fixture(scope="function")
    def run_launcher(
        self, cluster_provider, helm_namespace
    ):  # pylint: disable=redefined-outer-name,unused-argument

        return CeleryK8sRunLauncher(
            instance_config_map="dagster-instance",
            postgres_password_secret="dagster-postgresql-secret",
            dagster_home="/opt/dagster/dagster_home",
            load_incluster_config=False,
            kubeconfig_file=cluster_provider.kubeconfig_file,
        )

    @pytest.fixture(scope="function")
    def pipeline_name(self):  # pylint: disable=arguments-differ
        return "demo_pipeline_celery"

    @pytest.fixture(scope="function", name="run_config")
    def run_config(
        self, dagster_docker_image, helm_namespace
    ):  # pylint: disable=arguments-differ, redefined-outer-name
        path_to_run_config = os.path.join(test_project_environments_path(), "env.yaml")

        # Since we are running on celery-k8s, we need a distributed intermediate store.
        # Therefore, we pull in this configuration to use s3 for storage
        path_to_s3_run_config = os.path.join(test_project_environments_path(), "env_s3.yaml")

        # We also need celery executor specific configuration
        celery_executor_config = get_celery_engine_config(
            dagster_docker_image, job_namespace=helm_namespace
        )

        run_config = merge_dicts(
            merge_yamls([path_to_run_config, path_to_s3_run_config]), celery_executor_config
        )
        return run_config

    @pytest.fixture(scope="function", name="subset_run_config")
    def subset_run_config(
        self, dagster_docker_image, helm_namespace
    ):  # pylint: disable=arguments-differ, redefined-outer-name
        path_to_run_config = os.path.join(test_project_environments_path(), "env_subset.yaml")

        # Since we are running on celery-k8s, we need a distributed intermediate store.
        # Therefore, we pull in this configuration to use s3 for storage
        path_to_s3_run_config = os.path.join(test_project_environments_path(), "env_s3.yaml")

        # We also need celery executor specific configuration
        celery_executor_config = get_celery_engine_config(
            dagster_docker_image, job_namespace=helm_namespace
        )

        run_config = merge_dicts(
            merge_yamls([path_to_run_config, path_to_s3_run_config]), celery_executor_config
        )
        return run_config

    @pytest.fixture(scope="function", name="invalid_run_config")
    def invalid_run_config(
        self, dagster_docker_image, helm_namespace
    ):  # pylint: disable=arguments-differ, redefined-outer-name
        base = {"blah blah this is wrong": {}}
        celery_executor_config = get_celery_engine_config(
            dagster_docker_image, job_namespace=helm_namespace
        )

        return merge_dicts(base, celery_executor_config)
