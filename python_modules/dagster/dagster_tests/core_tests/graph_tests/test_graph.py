from dagster import execute_pipeline, pipeline, solid
from dagster.core.definitions.decorators.graph import graph
from dagster.core.definitions.graph import GraphDefinition


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
