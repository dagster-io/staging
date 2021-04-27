from dagster import ModeDefinition, fs_io_manager, pipeline, solid
from dagster.core.definitions import executor
from dagster.core.definitions.executor import multiple_process_executor_requirements
from dagster.core.definitions.reconstructable import ReconstructablePipeline
from dagster.core.execution.api import create_execution_plan, execute_run
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.test_utils import instance_for_test


@executor(
    name="new-multiprocess",
    config_schema={},
    requirements=multiple_process_executor_requirements(),
)
def multiprocess_executor(_):
    from dagster.core.executor.extendable import CoreExecutor
    from dagster.core.executor.extendable.step_handler import MultiprocessStepHandler

    return CoreExecutor(MultiprocessStepHandler())


@solid
def a_solid(_):
    return 0


@solid
def b_solid(_, _value):
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
        assert instance.get_run_by_id(pipeline_run.run_id).status == PipelineRunStatus.SUCCESS


def test_in_process_executor():
    assert_pipeline_runs()
