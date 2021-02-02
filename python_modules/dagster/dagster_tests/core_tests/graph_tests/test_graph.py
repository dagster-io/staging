from dagster import solid
from dagster.core.definitions.decorators.graph import graph
from dagster.core.definitions.graph import GraphDefinition
from dagster.core.execution.api import execute_node


def test_basic_graph():
    @solid
    def emit_one(_):
        return 1

    @solid
    def add(_, x, y):
        return x + y

    @graph
    def add_one(a):
        return add(emit_one(), a)

    assert isinstance(add_one, GraphDefinition)

    result = execute_node(add_one, run_config={"solids": {"add": {"inputs": {"y": {"value": 5}}}}})

    assert result.success
    assert result.output_for_solid("add") == 6
