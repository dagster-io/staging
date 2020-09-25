import os

import pytest
from dagster_k8s.test import wait_for_job_and_get_raw_logs
from dagster_k8s_test_infra.integration_utils import ReOriginatedExternalPipelineForTest
from dagster_test.test_project import (
    get_test_project_external_pipeline,
    test_project_environments_path,
)

from dagster.core.test_utils import create_run_for_test
from dagster.utils.yaml_utils import load_yaml_from_path


class TestK8sIntegration:
    """
    Base Class meant to be subclassed
    """

    @pytest.fixture(name="run_config", params=[])
    def run_config(self, request):
        with request.param() as s:
            yield s

    def test_launch_run(
        self,
        # From conftest
        dagster_instance,
        helm_namespace,
        # From class fixture
        run_config,
    ):
        pipeline_name = "demo_pipeline"

        # Create pipeline run
        external_pipeline = get_test_project_external_pipeline(pipeline_name)
        pipeline_run = create_run_for_test(
            dagster_instance, pipeline_name=pipeline_name, run_config=run_config, mode="default"
        )

        # Launch pipeline
        reoriginated_external_pipeline = ReOriginatedExternalPipelineForTest(external_pipeline)
        dagster_instance.launch_run(pipeline_run.run_id, reoriginated_external_pipeline)

        # Verify run was successful
        result = wait_for_job_and_get_raw_logs(
            job_name="dagster-run-%s" % pipeline_run.run_id, namespace=helm_namespace
        )
        assert "PIPELINE_SUCCESS" in result, "no match, result: {}".format(result)


class TestDagsterK8sIntegration(TestK8sIntegration):
    __test__ = True

    @pytest.fixture(scope="function", name="run_config")
    def run_config(self):  # pylint: disable=arguments-differ
        path_to_run_config = os.path.join(test_project_environments_path(), "env.yaml")
        run_config = load_yaml_from_path(path_to_run_config)
        return run_config
