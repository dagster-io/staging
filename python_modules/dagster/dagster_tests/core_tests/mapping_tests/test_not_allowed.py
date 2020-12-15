import pytest
from dagster import (
    DagsterInvalidDefinitionError,
    OutputDefinition,
    composite_solid,
    pipeline,
    solid,
)
from dagster.experimental import DynamicOutput, DynamicOutputDefinition


@solid(output_defs=[DynamicOutputDefinition()])
def dynamic_solid(_):
    yield DynamicOutput(1, mapping_key="1")
    yield DynamicOutput(2, mapping_key="2")


@solid
def echo(_, x):
    return x


@solid
def add(_, x, y):
    return x + y


def test_composite():
    with pytest.raises(
        DagsterInvalidDefinitionError, match="Definition types must align",
    ):

        @composite_solid(output_defs=[OutputDefinition()])
        def _should_fail():

            for x in dynamic_solid():
                return x

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match=" must be a DynamicOutputDefinition since it is downstream of dynamic output",
    ):

        @composite_solid(output_defs=[OutputDefinition()])
        def _should_fail():
            # maybe this?
            #
            # items = dynamic_solid()
            # return items.map(echo)

            for num in dynamic_solid():
                # force a yield ?
                return echo(num)


def test_fan_in():
    with pytest.raises(
        DagsterInvalidDefinitionError,
        match='Problematic dependency on dynamic output "dynamic_solid:result"',
    ):

        @pipeline
        def _should_fail():
            numbers = []
            for num in dynamic_solid():
                numbers.append(num)
            echo(numbers)


def test_multi_direct():
    with pytest.raises(
        DagsterInvalidDefinitionError, match="cannot be downstream of more than one dynamic output",
    ):

        @pipeline
        def _should_fail():
            for x in dynamic_solid():
                for y in dynamic_solid():
                    add(x, y)


def test_multi_indirect():
    with pytest.raises(
        DagsterInvalidDefinitionError, match="cannot be downstream of more than one dynamic output",
    ):

        @pipeline
        def _should_fail():
            for y in dynamic_solid():
                x = echo(y)
                for z in dynamic_solid():
                    add(z, x)


def test_multi_composite_out():
    with pytest.raises(
        DagsterInvalidDefinitionError, match="cannot be downstream of more than one dynamic output",
    ):

        @composite_solid(output_defs=[DynamicOutputDefinition()])
        def composed_echo():
            for x in dynamic_solid():
                return echo(x)

        @pipeline
        def _should_fail():
            for y in dynamic_solid():
                for z in composed_echo():
                    add(z, y)


def test_multi_composite_in():
    with pytest.raises(
        DagsterInvalidDefinitionError,
        match='cannot be downstream of dynamic output "dynamic_solid:result" since input "a" maps to a solid that is already downstream of another dynamic output',
    ):

        @composite_solid
        def composed_add(a):
            for b in dynamic_solid():
                add(a, b)

        @pipeline
        def _should_fail():
            for z in dynamic_solid():
                y = echo(z)
                composed_add(y)


def test_direct_dep():
    @solid(output_defs=[DynamicOutputDefinition()])
    def dynamic_add(_, x):
        yield DynamicOutput(x + 1, mapping_key="1")
        yield DynamicOutput(x + 2, mapping_key="2")

    @pipeline
    def _is_fine():
        for x in dynamic_solid():
            dynamic_add(x)

    with pytest.raises(
        DagsterInvalidDefinitionError, match="cannot be downstream of more than one dynamic output",
    ):

        @pipeline
        def _should_fail():
            for x in dynamic_solid():
                for y in dynamic_add(x):
                    echo(y)
