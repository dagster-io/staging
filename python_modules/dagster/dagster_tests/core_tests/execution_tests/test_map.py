import pytest
from dagster_graphql.implementation.resume_retry import get_retry_steps_from_execution_plan

from dagster import (
    Bool,
    DagsterInstance,
    InputDefinition,
    String,
    execute_pipeline,
    lambda_solid,
    pipeline,
    reconstructable,
    reexecute_pipeline,
    seven,
    solid,
)
from dagster.core.definitions.events import SpecialOutput
from dagster.core.definitions.output import SpecialOutputDefinition
from dagster.core.execution.api import create_execution_plan

# @solid(
#     input_defs=[
#         InputDefinition(name='should_raise', dagster_type=Bool, default_value=False),
#         InputDefinition(name='y', dagster_type=String),
#     ]
# )
# def echo(context, y, should_raise):
#     if should_raise and y == 2:
#         run = context.instance.get_run_by_id(context.run_id)
#         if run.parent_run_id is None:
#             raise Exception()
#     return y


# @solid(
#     input_defs=[
#         InputDefinition(name='should_raise', dagster_type=Bool, default_value=False),
#         InputDefinition(name='y', dagster_type=String),
#     ]
# )
# def echo_again(context, y, should_raise):
#     if should_raise and y == 2:
#         run = context.instance.get_run_by_id(context.run_id)
#         if run.parent_run_id is None:
#             raise Exception()
#     return y


@solid(output_defs=[SpecialOutputDefinition()])
def emit(_):
    for i in range(3):
        yield SpecialOutput(i, str(i))


@lambda_solid
def echo(y):
    return y


@pipeline
def multi_map_pipe():
    echo(echo(emit()))


def test_map():
    @lambda_solid
    def echo(y):
        return y

    @pipeline
    def map_pipe():
        echo(echo(emit()))

    result = execute_pipeline(map_pipe)
    assert result.success


def test_map_multi():
    with seven.TemporaryDirectory() as tmp_dir:
        result = execute_pipeline(
            reconstructable(multi_map_pipe),
            run_config={'storage': {'filesystem': {}}, 'execution': {'multiprocess': {}},},
            instance=DagsterInstance.local_temp(tmp_dir),
        )
        assert result.success


def test_reexec_1():
    @solid
    def echo(context, y):
        if y == 2:
            run = context.instance.get_run_by_id(context.run_id)
            if run.parent_run_id is None:
                raise Exception()
        return y

    @solid
    def echo_again(context, y):
        return y

    @pipeline
    def map_pipe():
        echo_again(echo(emit()))

    with seven.TemporaryDirectory() as tmp_dir:
        instance = DagsterInstance.local_temp(tmp_dir)
        result = execute_pipeline(
            map_pipe,
            run_config={'storage': {'filesystem': {}}},
            instance=instance,
            raise_on_error=False,
        )
        parent_run_id = result.run_id
        # execution_plan = ExternalExecutionPlan(execution_plan_snapshot=ExecutionPlanSnapshot(), represented_pipeline=)

        # step_keys_to_execute = get_retry_steps_from_execution_plan(
        #     instance=instance, execution_plan=execution_plan, parent_run_id=parent_run_id
        # )

        reexec_result = reexecute_pipeline(
            map_pipe,
            run_config={'storage': {'filesystem': {}}},
            parent_run_id=parent_run_id,
            step_keys_to_execute=["echo[emit.compute:result:2].compute"],
            instance=instance,
        )
        assert reexec_result.success


def test_reexec_2():
    @solid
    def echo(context, y):
        return y

    @solid
    def echo_again(context, y):
        if y == 2:
            run = context.instance.get_run_by_id(context.run_id)
            if run.parent_run_id is None:
                raise Exception()
        return y

    @pipeline
    def map_pipe():
        echo_again(echo(emit()))

    with seven.TemporaryDirectory() as tmp_dir:
        instance = DagsterInstance.local_temp(tmp_dir)
        result = execute_pipeline(
            map_pipe,
            run_config={'storage': {'filesystem': {}}},
            instance=instance,
            raise_on_error=False,
        )
        parent_run_id = result.run_id

        reexec_result = reexecute_pipeline(
            map_pipe,
            run_config={'storage': {'filesystem': {}}},
            parent_run_id=parent_run_id,
            step_keys_to_execute=["echo_again[emit.compute:result:2].compute"],
            instance=instance,
        )
        assert reexec_result.success
