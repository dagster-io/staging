import pytest
from dagster import execute_pipeline, graph, op, resource
from dagster.core.execution.api import create_execution_plan
from dagster.core.test_utils import instance_for_test
from docs_snippets.guides.dagster.memoization.memoization import (
    OpAndResourcesSourceHashStrategy,
    OpSourceHashStrategy,
    disabled_result,
    emit_number_job,
    emit_number_pipeline,
)


@pytest.mark.parametrize("strategy", [OpSourceHashStrategy, OpAndResourcesSourceHashStrategy])
def test_op_source_strategy(strategy):

    recorder = []

    @resource
    def foo_resource():
        return "foo"

    @op(required_resource_keys={"foo"})
    def my_op():
        recorder.append("True")
        return 5

    @graph
    def my_graph():
        my_op()

    my_job = my_graph.to_job(version_strategy=strategy(), resource_defs={"foo": foo_resource})

    with instance_for_test() as instance:
        for _ in range(2):  # Execute twice and ensure that my_op is not re-executed.
            result = my_job.execute_in_process(instance=instance)
            assert result.success

            assert len(recorder) == 1


def test_emit_number_job():

    with instance_for_test() as instance:
        result = emit_number_job.execute_in_process(instance=instance)
        assert result.success

        assert (
            len(create_execution_plan(emit_number_job, instance=instance).step_keys_to_execute) == 0
        )


def test_disabled_job_result():
    assert disabled_result.success


def test_memoized_pipeline_example():
    with instance_for_test() as instance:
        result = execute_pipeline(emit_number_pipeline, instance=instance)
        assert result.success

        assert (
            len(create_execution_plan(emit_number_pipeline, instance=instance).step_keys_to_execute)
            == 0
        )
