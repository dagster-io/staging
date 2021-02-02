from dagster import execute_pipeline, pipeline, solid
from dagster.core.definitions.decorators.graph import graph
from dagster.core.definitions.graph import GraphDefinition
from dagster.core.execution.api import execute_node


def get_solids():
    @solid
    def emit_one(_):
        return 1

    @solid
    def add(_, x, y):
        return x + y

    return emit_one, add


def test_basic_graph():
    emit_one, add = get_solids()

    @graph
    def get_two():
        return add(emit_one(), emit_one())

    assert isinstance(get_two, GraphDefinition)

    @pipeline
    def call_graph():
        get_two()

    result = execute_pipeline(call_graph)
    assert result.success
    assert result.result_for_handle("get_two").output_value("result") == 2


def test_composite_graph():
    emit_one, add = get_solids()

    @graph
    def wrap_emit_one():
        return emit_one()

    @graph
    def get_two():
        return add(wrap_emit_one(), emit_one())

    result = execute_node(get_two)
    assert result.success
    assert result.result_for_handle("get_two").output_value("result") == 2


def test_graph_with_inputs():
    emit_one, add = get_solids()

    @graph
    def add_one(x):
        return add(x, emit_one())

    result = execute_node(add_one, input_values={"x": 5})

    assert result.success
    assert result.result_for_handle("add_one").output_value("result") == 6
