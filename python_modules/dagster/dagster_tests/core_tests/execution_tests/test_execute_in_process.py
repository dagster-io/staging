import re

import pytest
from dagster import DagsterInvalidDefinitionError, solid
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

    result = execute_in_process(emit_one, output_recording_enabled=True)

    assert result.success
    assert result.output_dict[("emit_one", "result")] == 1


def test_execute_graph():
    emit_one, add = get_solids()

    @graph
    def emit_two():
        return add(emit_one(), emit_one())

    @graph
    def emit_three():
        return add(emit_two(), emit_one())

    result = execute_in_process(emit_three, output_recording_enabled=True)

    assert result.success

    assert result.output_dict[("emit_three.add", "result")] == 3
    assert result.output_dict[("emit_three.emit_two.add", "result")] == 2
    assert result.output_dict[("emit_three.emit_one", "result")] == 1
    assert result.output_dict[("emit_three.emit_two.emit_one", "result")] == 1
    assert result.output_dict[("emit_three.emit_two.emit_one_2", "result")] == 1


def test_execute_solid_with_inputs():
    @solid
    def add_one(_, x):
        return 1 + x

    result = execute_in_process(add_one, input_values={"x": 5}, output_recording_enabled=True)
    assert result.success

    assert result.output_dict[("add_one", "result")] == 6


def test_execute_graph_with_inputs():
    emit_one, add = get_solids()

    @graph
    def add_one(x):
        return add(x, emit_one())

    result = execute_in_process(add_one, input_values={"x": 5}, output_recording_enabled=True)
    assert result.success
    assert result.output_dict[("add_one.add", "result")] == 6
    assert result.output_dict[("add_one.emit_one", "result")] == 1


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
