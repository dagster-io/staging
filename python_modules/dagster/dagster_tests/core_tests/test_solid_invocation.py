from typing import Any, NamedTuple, Optional

from dagster import InputDefinition, Output, OutputDefinition, solid
from dagster.core.definitions.resource import Resources
from dagster.core.execution.build_resources import build_resources


class SolidInvocationContext(NamedTuple):
    resources: Optional[Resources] = None


def test_solid_invocation():
    @solid
    def basic_solid(_):
        return 5

    result = basic_solid(SolidInvocationContext())
    assert result == 5


def test_solid_invocation_multiple_outputs():
    @solid(
        output_defs=[OutputDefinition(int, name="result_1"), OutputDefinition(int, name="result_2")]
    )
    def solid_with_multiple_outputs(_):
        yield Output(output_name="result_1", value=5)
        yield Output(output_name="result_2", value=6)

    result = solid_with_multiple_outputs(SolidInvocationContext())
    result


def test_solid_invocation_with_inputs():
    @solid(input_defs=[InputDefinition("x", int), InputDefinition("y", int)])
    def basic_solid_with_inputs(_, x, y):
        assert x == 5
        assert y == 6
        return x + y

    result = basic_solid_with_inputs(SolidInvocationContext(), 5, 6)
    assert result == 11
