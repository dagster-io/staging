import re

import pytest
from dagster import DagsterInvalidDefinitionError, Field, InputDefinition, resource, solid
from dagster.core.definitions.decorators.graph import graph
from dagster.core.errors import DagsterInvalidConfigError
from dagster.core.execution.execute_in_process import FromInputConfig, execute_in_process
from dagster.core.storage.io_manager import IOManager, io_manager
from dagster.core.storage.mem_io_manager import InMemoryIOManager, mem_io_manager
from dagster.experimental import DynamicOutput, DynamicOutputDefinition


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

    result = execute_in_process(emit_one)

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

    result = execute_in_process(emit_three)

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

    result = execute_in_process(add_one, input_values={"x": 5})
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


def test_execute_solid_with_required_resources():
    @solid(required_resource_keys={"foo"})
    def solid_requires_resource(context):
        assert context.resources.foo == "bar"
        return context.resources.foo

    # provide the resource value directly
    result = execute_in_process(solid_requires_resource, resources={"foo": "bar"})
    assert result.success
    assert result.output_values["result"] == "bar"

    @resource
    def foo_resource(_):
        return "bar"

    # provide the resource as a definition to be instantiated
    result = execute_in_process(solid_requires_resource, resources={"foo": foo_resource})
    assert result.success
    assert result.output_values["result"] == "bar"


def test_execute_solid_requires_config():
    @solid(config_schema={"foo": str})
    def solid_requires_config(context):
        assert context.solid_config["foo"] == "bar"
        return context.solid_config["foo"]

    result = execute_in_process(solid_requires_config, solid_config={"foo": "bar"})
    assert result.success
    assert result.output_values["result"] == "bar"


def test_execute_graph_solids_require_config():
    @solid(config_schema={"foo": str})
    def solid_requires_config(context):
        assert context.solid_config["foo"] == "bar"
        return context.solid_config["foo"]

    @graph
    def graph_solids_require_config():
        return solid_requires_config()

    result = execute_in_process(
        graph_solids_require_config,
        composed_config={"solid_requires_config": {"config": {"foo": "bar"}}},
    )

    assert result.success
    assert result.output_values["result"] == "bar"


def test_execute_graph_with_required_io_manager():
    emit_one, add = get_solids()

    @graph
    def get_two():
        return add(emit_one(), emit_one())

    result = execute_in_process(get_two, resources={"io_manager": InMemoryIOManager()})
    assert result.success
    assert result.output_values["result"] == 2

    result = execute_in_process(get_two, resources={"io_manager": mem_io_manager})
    assert result.success
    assert result.output_values["result"] == 2


def test_execute_solid_with_io_config_io_manager():
    @io_manager(input_config_schema={"test_input": str})
    def basic_io_manager(_):
        class BasicIOManager(IOManager):
            def handle_output(self, _context, _obj):
                pass

            def load_input(self, context):
                assert context.config["test_input"] == "bar"

        return BasicIOManager()

    @solid(input_defs=[InputDefinition("_x", root_manager_key="io_manager")])
    def noop_solid_takes_input(_, _x):
        pass

    result = execute_in_process(
        noop_solid_takes_input,
        resources={"io_manager": basic_io_manager},
        input_values=FromInputConfig({"_x": {"test_input": "bar"}}),
    )
    assert result.success


def test_execute_in_process_resource_requires_config():
    @resource(
        config_schema={"foo": str, "animal": Field(str, default_value="dog", is_required=False)}
    )
    def basic_resource(init_context):
        assert init_context.resource_config["foo"] == "bar"
        assert init_context.resource_config["animal"] == "dog"

    @solid(required_resource_keys={"basic_resource"})
    def basic_solid(_):
        pass

    result = execute_in_process(
        basic_solid, resources={"basic_resource": basic_resource.configured({"foo": "bar"})}
    )

    assert result.success

    with pytest.raises(
        DagsterInvalidConfigError,
        match="Error in config for pipeline ephemeral_basic_solid_node_pipeline",
    ):
        execute_in_process(
            basic_solid,
            resources={"basic_resource": basic_resource},
        )
