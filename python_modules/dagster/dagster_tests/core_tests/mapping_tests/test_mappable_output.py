import pytest
from dagster import execute_solid, pipeline, solid
from dagster.core.errors import DagsterInvalidDefinitionError, DagsterInvariantViolationError
from dagster.experimental import MappableOutput, MappableOutputDefinition


def test_basic():
    @solid(output_defs=[MappableOutputDefinition()])
    def should_work(_):
        yield MappableOutput(1, mapping_key="1")
        yield MappableOutput(2, mapping_key="2")

    result = execute_solid(should_work)

    assert result.success
    assert len(result.get_output_events_for_compute()) == 2
    assert len(result.compute_output_events_dict["result"]) == 2
    assert result.output_values == {"result": {"1": 1, "2": 2}}
    assert result.output_value() == {"1": 1, "2": 2}


def test_fails_without_def():
    @solid
    def should_fail(_):
        yield MappableOutput(True, mapping_key="foo")

    with pytest.raises(
        DagsterInvariantViolationError, match="did not use MappableOutputDefinition"
    ):
        execute_solid(should_fail)


def test_fails_dupe_keys():
    @solid(output_defs=[MappableOutputDefinition()])
    def should_fail(_):
        yield MappableOutput(True, mapping_key="dunk")
        yield MappableOutput(True, mapping_key="dunk")

    with pytest.raises(DagsterInvariantViolationError, match='mapping_key "dunk" multiple times'):
        execute_solid(should_fail)


def test_invalid_mapping_keys():
    with pytest.raises(DagsterInvalidDefinitionError):
        MappableOutput(True, mapping_key="")

    with pytest.raises(DagsterInvalidDefinitionError):
        MappableOutput(True, mapping_key="?")

    with pytest.raises(DagsterInvalidDefinitionError):
        MappableOutput(True, mapping_key="foo.baz")


def test_temp_fail_on_dep():
    # to be removed in upcoming diff

    @solid(output_defs=[MappableOutputDefinition()])
    def should_work(_):
        yield MappableOutput(1, mapping_key="1")
        yield MappableOutput(2, mapping_key="2")

    @solid
    def echo(_, x):
        return x

    with pytest.raises(DagsterInvalidDefinitionError, match="not yet supported"):

        @pipeline
        def _uh_oh():
            echo(should_work())
