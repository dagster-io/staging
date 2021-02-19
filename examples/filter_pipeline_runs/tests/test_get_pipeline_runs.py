from dagster import execute_pipeline, pipeline, solid
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import instance_for_test
from filter_pipeline_runs.filter_pipeline_runs.get_pipeline_runs import get_pipeline_runs


@solid
def hello_error(_):
    raise Exception


@pipeline
def get_error_pipeline():
    hello_error()


def test_get_failed_runs():
    with instance_for_test() as instance:
        res = execute_pipeline(
            get_error_pipeline,
            instance=instance,
            raise_on_error=False,
        )
        assert not res.success

        run_list = get_pipeline_runs(instance)

        assert run_list[0].pipeline_name == "get_error_pipeline"
        assert run_list[0].status == PipelineRunStatus.FAILURE
