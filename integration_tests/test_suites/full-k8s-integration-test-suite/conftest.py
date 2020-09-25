# pylint: disable=unused-import
import os

import docker
import kubernetes
import pytest
from dagster_k8s.launcher import K8sRunLauncher
from dagster_k8s.scheduler import K8sScheduler
from dagster_k8s_test_infra.cluster import (
    dagster_instance,
    dagster_instance_with_k8s_scheduler,
    define_cluster_provider_fixture,
)
from dagster_k8s_test_infra.helm import helm_namespace
from dagster_k8s_test_infra.integration_utils import image_pull_policy
from dagster_test.test_project import build_and_tag_test_image, test_project_docker_image

from dagster import seven
from dagster.core.instance import DagsterInstance

IS_BUILDKITE = os.getenv("BUILDKITE") is not None


@pytest.fixture(scope="session", autouse=True)
def dagster_home():
    old_env = os.getenv("DAGSTER_HOME")
    os.environ["DAGSTER_HOME"] = "/opt/dagster/dagster_home"
    yield
    if old_env is not None:
        os.environ["DAGSTER_HOME"] = old_env


cluster_provider = define_cluster_provider_fixture(
    additional_kind_images=["docker.io/bitnami/rabbitmq", "docker.io/bitnami/postgresql"]
)


# See: https://stackoverflow.com/a/31526934/324449
def pytest_addoption(parser):
    # We catch the ValueError to support cases where we are loading multiple test suites, e.g., in
    # the VSCode test explorer. When pytest tries to add an option twice, we get, e.g.
    #
    #    ValueError: option names {'--cluster-provider'} already added

    # Use kind or some other cluster provider?
    try:
        parser.addoption("--cluster-provider", action="store", default="kind")
    except ValueError:
        pass

    # Specify an existing kind cluster name to use
    try:
        parser.addoption("--kind-cluster", action="store")
    except ValueError:
        pass

    # Keep resources around after tests are done
    try:
        parser.addoption("--no-cleanup", action="store_true", default=False)
    except ValueError:
        pass

    # Use existing Helm chart/namespace
    try:
        parser.addoption("--existing-helm-namespace", action="store")
    except ValueError:
        pass


# # Minimal
# from dagster_k8s_test_infra.cluster import define_cluster_provider_fixture

# cluster_provider = define_cluster_provider_fixture(
#     additional_kind_images=["docker.io/bitnami/rabbitmq", "docker.io/bitnami/postgresql"]
# )
