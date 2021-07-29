import re
import tempfile

import pytest
from dagster import (
    DagsterInvalidDefinitionError,
    DynamicOutput,
    DynamicOutputDefinition,
    resource,
    solid,
)
from dagster.core.definitions.decorators.graph import graph
from dagster.core.definitions.version_strategy import VersionStrategy
from dagster.core.execution.execute import execute_in_process
from dagster.core.storage.memoizable_io_manager import versioned_filesystem_io_manager
from dagster.core.test_utils import instance_for_test


def get_solids():
    @solid
    def emit_one(_):
        return 1

    @solid
    def add(_, x, y):
        return x + y

    return emit_one, add


def test_execute_solid():
    emit_one, _ = get_solids()

    result = execute_in_process(emit_one, output_capturing_enabled=True)

    assert result.success
    assert result.output_values["result"] == 1


def test_execute_graph():
    emit_one, add = get_solids()

    @graph
    def emit_two():
        return add(emit_one(), emit_one())

    @graph
    def emit_three():
        return add(emit_two(), emit_one())

    result = emit_three.execute_in_process()

    assert result.success

    assert result.output_values["result"] == 3
    assert result.result_for_node("add").output_values["result"] == 3
    assert result.result_for_node("emit_two").output_values["result"] == 2
    assert result.result_for_node("emit_one").output_values["result"] == 1
    assert (
        result.result_for_node("emit_two").result_for_node("emit_one").output_values["result"] == 1
    )
    assert (
        result.result_for_node("emit_two").result_for_node("emit_one_2").output_values["result"]
        == 1
    )


def test_execute_solid_with_inputs():
    @solid
    def add_one(_, x):
        return 1 + x

    result = execute_in_process(add_one, input_values={"x": 5}, output_capturing_enabled=True)
    assert result.success

    assert result.output_values["result"] == 6


def test_execute_graph_with_inputs():
    emit_one, add = get_solids()

    @graph
    def add_one(x):
        return add(x, emit_one())

    result = execute_in_process(add_one, input_values={"x": 5})
    assert result.success
    assert result.output_values["result"] == 6
    assert result.result_for_node("emit_one").output_values["result"] == 1


def test_execute_graph_nonexistent_inputs():
    emit_one, add = get_solids()

    @graph
    def get_two():
        return add(emit_one(), emit_one())

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match=re.escape(
            'Invalid dependencies: graph "get_two" does not have input "x". Available inputs: []'
        ),
    ):
        execute_in_process(get_two, input_values={"x": 5})

    @graph
    def add_one(x):
        return add(x, emit_one())

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match=re.escape(
            'Invalid dependencies: graph "add_one" does not have input "y". '
            "Available inputs: ['x']"
        ),
    ):
        execute_in_process(add_one, input_values={"y": 5})


def test_execute_solid_nonexistent_inputs():
    emit_one, add = get_solids()

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match=re.escape(
            'Invalid dependencies: solid "emit_one" does not have input "x". Available inputs: []'
        ),
    ):
        execute_in_process(emit_one, input_values={"x": 5})

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match=re.escape(
            'Invalid dependencies: solid "add" does not have input "z". '
            "Available inputs: ['x', 'y']"
        ),
    ):
        execute_in_process(add, input_values={"z": 5})


def test_dynamic_output_solid():
    @solid(output_defs=[DynamicOutputDefinition()])
    def should_work(_):
        yield DynamicOutput(1, mapping_key="1")
        yield DynamicOutput(2, mapping_key="2")

    result = execute_in_process(should_work)
    assert result.success
    assert result.output_values["result"]["1"] == 1
    assert result.output_values["result"]["2"] == 2


def test_graph_with_required_resources():
    @solid(required_resource_keys={"a"})
    def basic_reqs(context):
        return context.resources.a

    @graph
    def basic_graph():
        return basic_reqs()

    result = basic_graph.execute_in_process(resources={"a": "foo"})
    assert result.output_values["result"] == "foo"

    @resource
    def basic_resource():
        return "bar"

    result = basic_graph.execute_in_process(resources={"a": basic_resource})
    assert result.output_values["result"] == "bar"


def test_execute_in_process_retrieve_memoized_results():

    with tempfile.TemporaryDirectory() as temp_dir:
        with instance_for_test(temp_dir=temp_dir) as instance:

            class MyVersionStrategy(VersionStrategy):
                def get_solid_version(self, _solid_def):
                    return "foo"

            recorder = []

            @solid
            def my_solid():
                recorder.append("entered")
                return 5

            @graph
            def my_graph():
                my_solid()

            my_job = my_graph.to_job(
                version_strategy=MyVersionStrategy(),
                resource_defs={"io_manager": versioned_filesystem_io_manager},
            )

            result = my_job.execute_in_process(instance=instance)
            assert result.success
            assert recorder == ["entered"]

            result = my_job.execute_in_process(instance=instance)
            assert result.success
            result_for_solid = result.result_for_node("my_solid")
            assert result_for_solid.output_values["result"] == 5

            # Ensure that the solid has not been re-run, that we are retrieving memoized outputs
            # only.
            assert recorder == ["entered"]
