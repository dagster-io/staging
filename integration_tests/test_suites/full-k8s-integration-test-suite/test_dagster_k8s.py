import os

import pytest
from dagster_test.test_project import test_project_environments_path
from test_integration_base import TestK8sIntegration

from dagster.utils.yaml_utils import load_yaml_from_path


class TestDagsterK8sIntegration(TestK8sIntegration):
    __test__ = True

    @pytest.fixture(scope="function", name="run_config")
    def run_config(self):  # pylint: disable=arguments-differ
        path_to_run_config = os.path.join(test_project_environments_path(), "env.yaml")
        run_config = load_yaml_from_path(path_to_run_config)
        return run_config
