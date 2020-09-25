# pylint: disable=unused-import
import os

import pytest
from dagster_k8s_test_infra.helm import helm_namespace  # This is a fixture

from dagster import seven

IS_BUILDKITE = os.getenv("BUILDKITE") is not None


@pytest.fixture(scope="session", autouse=True)
def dagster_home():
    old_env = os.getenv("DAGSTER_HOME")
    os.environ["DAGSTER_HOME"] = "/opt/dagster/dagster_home"
    yield
    if old_env is not None:
        os.environ["DAGSTER_HOME"] = old_env
