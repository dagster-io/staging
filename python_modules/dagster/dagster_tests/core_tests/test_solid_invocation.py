import asyncio

import pytest
from dagster import Output, pipeline, solid
from dagster.check import CheckError
from dagster.core.errors import (
    DagsterInvalidDefinitionError,
    DagsterInvalidPropertyError,
    DagsterSolidInvocationError,
)


def test_solid_invocation():
    @solid
    def basic_solid(_):
        return 5

    result = basic_solid()
    assert result == 5


def test_solid_invocation_with_resources():
    @solid(required_resource_keys={"foo"})
    def solid_requires_resources(_):
        pass

    # Ensure that a proper error is thrown when attempting to execute and no context is provided
    with pytest.raises(
        CheckError,
        match='Solid "solid_requires_resources" has required resources. '
        "Directly invoking solids that require resources is not yet supported.",
    ):
        solid_requires_resources()


def test_solid_invocation_with_config():
    @solid(config_schema={"foo": str})
    def solid_requires_config(context):
        assert context.solid_config["foo"] == "bar"
        return 5

    # Ensure that a proper error is thrown when attempting to execute and no context is provided
    with pytest.raises(
        CheckError,
        match='Solid "solid_requires_config" has required config schema, but no config has been '
        "provided.",
    ):
        solid_requires_config()

    result = solid_requires_config.configured({"foo": "bar"}, name="configured_solid")()
    assert result == 5


def test_solid_with_inputs():
    @solid
    def solid_with_inputs(_, x, y):
        assert x == 5
        assert y == 6
        return x + y

    assert solid_with_inputs(5, 6) == 11
    assert solid_with_inputs(x=5, y=6) == 11
    assert solid_with_inputs(5, y=6) == 11
    assert solid_with_inputs(y=6, x=5) == 11

    # Check for proper error when incorrect number of inputs is provided.
    with pytest.raises(CheckError, match='No value provided for required input "y".'):
        solid_with_inputs(5)


def test_failing_solid():
    @solid
    def solid_fails(_):
        raise Exception("Oh no!")

    with pytest.raises(
        DagsterSolidInvocationError,
        match='Error occurred while invoking solid "solid_fails":',
    ):
        solid_fails()


def test_attempted_invocation_in_composition():
    @solid
    def basic_solid(_, _x):
        pass

    msg = (
        "Must pass the output from previous solid invocations or inputs to the composition "
        "function as inputs when invoking solids during composition."
    )
    with pytest.raises(
        DagsterInvalidDefinitionError,
        match=msg,
    ):

        @pipeline
        def _pipeline_will_fail():
            basic_solid(5)

    with pytest.raises(
        DagsterInvalidDefinitionError,
        match=msg,
    ):

        @pipeline
        def _pipeline_will_fail_again():
            basic_solid(_x=5)


def test_async_solid():
    @solid
    async def aio_solid(_):
        await asyncio.sleep(0.01)
        return "done"

    assert aio_solid() == "done"


def test_async_gen_invocation():
    @solid
    async def aio_gen(_):
        await asyncio.sleep(0.01)
        yield Output("done")

    assert aio_gen() == "done"


def test_multiple_outputs_iterator():
    @solid
    def solid_multiple_outputs(_):
        yield Output(1, output_name="1")
        yield Output(2, output_name="2")

    assert solid_multiple_outputs() == {"1": 1, "2": 2}


@pytest.mark.parametrize(
    "property_or_method_name,val_to_pass",
    [
        ("resources", None),
        ("pipeline_run", None),
        ("instance", None),
        ("step_launcher", None),
        ("run_config", None),
        ("pipeline_def", None),
        ("pipeline_name", None),
        ("mode_def", None),
        ("solid_handle", None),
        ("solid", None),
        ("has_tag", "foo"),
        ("get_tag", "foo"),
        ("get_step_execution_context", None),
    ],
)
def test_invalid_properties_on_context(property_or_method_name, val_to_pass):
    @solid
    def solid_fails_getting_property(context):
        result = getattr(context, property_or_method_name)
        # for the case where property_or_method_name is a method, getting an attribute won't cause
        # an error, but invoking the method should.
        result(val_to_pass) if val_to_pass else result()  # pylint: disable=W0106

    with pytest.raises(DagsterInvalidPropertyError):
        solid_fails_getting_property()
