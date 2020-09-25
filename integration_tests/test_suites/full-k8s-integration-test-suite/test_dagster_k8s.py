import os

import pytest
from dagster_k8s.launcher import K8sRunLauncher
from dagster_k8s_test_infra.cluster import define_cluster_provider_fixture
from dagster_k8s_test_infra.integration_utils import image_pull_policy
from dagster_test.test_project import test_project_docker_image, test_project_environments_path
from test_integration_base import TestK8sIntegration

from dagster.utils.yaml_utils import load_yaml_from_path

cluster_provider = define_cluster_provider_fixture(
    additional_kind_images=["docker.io/bitnami/rabbitmq", "docker.io/bitnami/postgresql"]
)


class TestDagsterK8sIntegration(TestK8sIntegration):
    __test__ = True

    @pytest.fixture(scope="function")
    def run_launcher(
        self, cluster_provider, helm_namespace
    ):  # pylint: disable=redefined-outer-name,unused-argument

        return K8sRunLauncher(
            image_pull_secrets=[{"name": "element-dev-key"}],
            service_account_name="dagit-admin",
            instance_config_map="dagster-instance",
            postgres_password_secret="dagster-postgresql-secret",
            dagster_home="/opt/dagster/dagster_home",
            job_image=test_project_docker_image(),
            load_incluster_config=False,
            kubeconfig_file=cluster_provider.kubeconfig_file,
            image_pull_policy=image_pull_policy(),
            job_namespace=helm_namespace,
            env_config_maps=["dagster-pipeline-env", "test-env-configmap"],
            env_secrets=["test-env-secret"],
        )

    @pytest.fixture(scope="function")
    def pipeline_name(self):  # pylint: disable=arguments-differ
        return "demo_pipeline"

    @pytest.fixture(scope="function", name="run_config")
    def run_config(self):  # pylint: disable=arguments-differ
        path_to_run_config = os.path.join(test_project_environments_path(), "env.yaml")
        run_config = load_yaml_from_path(path_to_run_config)
        return run_config

    @pytest.fixture(scope="function", name="subset_run_config")
    def subset_run_config(self):  # pylint: disable=arguments-differ
        path_to_run_config = os.path.join(test_project_environments_path(), "env_subset.yaml")
        run_config = load_yaml_from_path(path_to_run_config)
        return run_config

    @pytest.fixture(scope="function", name="invalid_run_config")
    def invalid_run_config(self):  # pylint: disable=arguments-differ
        return {"blah blah this is wrong": {}}
