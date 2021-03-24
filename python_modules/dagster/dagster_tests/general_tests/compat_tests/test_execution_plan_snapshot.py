import os

from dagster import Field, ModeDefinition, fs_io_manager, pipeline, solid
from dagster.core.definitions.pipeline_base import InMemoryPipeline
from dagster.core.execution.api import execute_run
from dagster.core.instance import DagsterInstance
from dagster.core.instance.ref import InstanceRef
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.experimental import DynamicOutput, DynamicOutputDefinition
from dagster.utils import file_relative_path
from dagster.utils.test import copy_directory


@solid
def multiply_by_two(context, y):
    context.log.info("multiply_by_two is returning " + str(y * 2))
    return y * 2


@solid
def multiply_inputs(context, y, ten):
    context.log.info("multiply_inputs is returning " + str(y * ten))
    return y * ten


@solid
def emit_ten(_):
    return 10


@solid
def echo(_, x: int) -> int:
    return x


@solid(
    output_defs=[DynamicOutputDefinition()],
    config_schema={
        "range": Field(int, is_required=False, default_value=3),
    },
)
def emit(context):
    for i in range(context.solid_config["range"]):
        yield DynamicOutput(value=i, mapping_key=str(i))


@solid
def sum_numbers(_, nums):
    return sum(nums)


@solid(output_defs=[DynamicOutputDefinition()])
def dynamic_echo(_, nums):
    for x in nums:
        yield DynamicOutput(value=x, mapping_key=str(x))


@pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": fs_io_manager})])
def dynamic_pipeline():
    numbers = emit()
    dynamic = numbers.map(lambda num: multiply_by_two(multiply_inputs(num, emit_ten())))
    n = multiply_by_two.alias("double_total")(sum_numbers(dynamic.collect()))
    echo(n)


# Verify that an previously generated execution plan snapshot can still execute a
# pipeline successfully
def test_execution_plan_snapshot_backcompat():
    src_dir = file_relative_path(__file__, "test_execution_plan_snapshots/")
    snapshot_dirs = [f for f in os.listdir(src_dir) if not os.path.isfile(os.path.join(src_dir, f))]
    for snapshot_dir_path in snapshot_dirs:
        print(f"Executing a saved run from {snapshot_dir_path}")  # pylint: disable=print-call

        with copy_directory(os.path.join(src_dir, snapshot_dir_path)) as test_dir:
            with DagsterInstance.from_ref(InstanceRef.from_dir(test_dir)) as instance:
                runs = instance.get_runs()
                assert len(runs) == 1

                run = runs[0]
                assert run.status == PipelineRunStatus.NOT_STARTED
                result = execute_run(
                    InMemoryPipeline(dynamic_pipeline), run, instance, raise_on_error=True
                )
                assert result.success


# To generate a new snapshot against your local DagsterInstance (run this script in python
# after wiping your sqlite instance, then copy the 'history' directory into a new subfolder
# in the test_execution_plan_snapshots folder)
if __name__ == "__main__":
    with DagsterInstance.get() as gen_instance:
        empty_runs = gen_instance.get_runs()
        assert len(empty_runs) == 0
        gen_instance.create_run_for_pipeline(
            pipeline_def=dynamic_pipeline,
            run_config={"solids": {"emit": {"config": {"range": 5}}}},
        )
