from dagster import (
    ModeDefinition,
    PipelineDefinition,
    execute_pipeline,
    fs_io_manager,
    pipeline,
    solid,
)
from dagster.core.definitions.executor import executor
from dagster.core.definitions.reconstructable import ReconstructablePipeline
from dagster.core.execution.api import create_execution_plan, execute_run
from dagster.core.execution.retries import RetryMode
from dagster.core.executor.extendable.step_handler.multiprocess import multiprocess_executor
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import instance_for_test


@solid
def a_solid(_):
    return 0


@solid
def b_solid(_, value):
    pass


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={"io_manager": fs_io_manager}, executor_defs=[multiprocess_executor]
        )
    ]
)
def foo_pipeline():
    return b_solid(a_solid())


def assert_pipeline_runs():

    with instance_for_test() as instance:
        run_config = {"execution": {"new-multiprocess": {}}}
        execution_plan = create_execution_plan(
            foo_pipeline,
            run_config,
        )
        pipeline_run = instance.create_run_for_pipeline(
            pipeline_def=foo_pipeline,
            execution_plan=execution_plan,
            run_config=run_config,
        )
        execute_run(
            ReconstructablePipeline.for_file(__file__, foo_pipeline.name),
            pipeline_run,
            instance,
            raise_on_error=True,
        )
        logs = instance.all_logs(pipeline_run.run_id)
        print(logs)
        assert instance.get_run_by_id(pipeline_run.run_id).status == PipelineRunStatus.SUCCESS


def test_in_process_executor():
    assert_pipeline_runs()
