import re

import pytest
from dagster import DagsterInvalidDefinitionError, DagsterInvariantViolationError, solid
from dagster.core.definitions.decorators.graph import graph
from dagster.core.execution.execute import execute_in_process


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

    with pytest.raises(
        DagsterInvariantViolationError, match="Cannot retrieve an inner result from a solid."
    ):
        result.result_for_handle("result_doesnt_exist")


def test_execute_graph():
    emit_one, add = get_solids()

    @graph
    def emit_two():
        return add(emit_one.alias("emit_one_1")(), emit_one.alias("emit_one_2")())

    @graph
    def emit_three():
        return add(emit_two(), emit_one())

    result = execute_in_process(emit_three)

    assert result.success
    assert result.output_values["result"] == 3
    assert result.result_for_handle("emit_two").output_values["result"] == 2
    assert result.result_for_handle("emit_one").output_values["result"] == 1
    assert (
        result.result_for_handle("emit_two").result_for_handle("emit_one_1").output_values["result"]
        == 1
    )
    assert (
        result.result_for_handle("emit_two").result_for_handle("emit_one_2").output_values["result"]
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
