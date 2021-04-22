import asyncio

import pytest
from dagster import Output, pipeline, solid
from dagster.check import CheckError
from dagster.core.errors import (
    DagsterInvalidConfigError,
    DagsterInvalidDefinitionError,
    DagsterInvalidPropertyError,
    DagsterSolidInvocationError,
)
from dagster.core.execution.context.invocation import get_solid_execution_context


def test_solid_invocation_no_arg():
    @solid
    def basic_solid():
        return 5

    result = basic_solid()
    assert result == 5


def test_solid_invocation_none_arg():
    @solid
    def basic_solid(_):
        return 5

    result = basic_solid(None)
    assert result == 5


def test_solid_invocation_with_resources():
    @solid(required_resource_keys={"foo"})
    def solid_requires_resources(context):
        assert context.resources.foo == "bar"
        return context.resources.foo

    # Ensure that a check invariant is raise when we attempt to invoke without context
    with pytest.raises(
        CheckError,
        match="Compute function of solid 'solid_requires_resources' has context argument, but no "
        "context was provided when invoking.",
    ):
        solid_requires_resources()

    # Ensure that error is raised when we attempt to invoke with a None context
    with pytest.raises(
        CheckError,
        match='Solid "solid_requires_resources" has required resources, but no context was '
        "provided.",
    ):
        solid_requires_resources(None)

    # Ensure that error is raised when we attempt to invoke with a context without the required
    # resource.
    context = get_solid_execution_context()
    with pytest.raises(
        CheckError,
        match='Solid "solid_requires_resources" requires resource "foo", but no resource '
        "with that key was found on the context.",
    ):
        solid_requires_resources(context)

    context = get_solid_execution_context(resources={"foo": "bar"})
    assert solid_requires_resources(context) == "bar"


def test_solid_invocation_with_config():
    @solid(config_schema={"foo": str})
    def solid_requires_config(context):
        assert context.solid_config["foo"] == "bar"
        return 5

    # Ensure that error is raised when attempting to execute and no context is provided
    with pytest.raises(
        CheckError,
        match="Compute function of solid 'solid_requires_config' has context argument, but no "
        "context was provided when invoking.",
    ):
        solid_requires_config()

    # Ensure that error is raised when we attempt to invoke with a None context
    with pytest.raises(
        CheckError,
        match='Solid "solid_requires_config" has required config schema, but no context was '
        "provided.",
    ):
        solid_requires_config(None)

    # Ensure that error is raised when context does not have the required config.
    context = get_solid_execution_context()
    with pytest.raises(
        DagsterInvalidConfigError,
        match="Error in config for solid",
    ):
        solid_requires_config(context)

    # Ensure that error is raised when attempting to execute and no context is provided, even when
    # configured
    with pytest.raises(
        CheckError,
        match="Compute function of solid 'configured_solid' has context argument, but no "
        "context was provided when invoking.",
    ):
        solid_requires_config.configured({"foo": "bar"}, name="configured_solid")()

    # Ensure that if you configure the solid, you can provide a none-context.
    result = solid_requires_config.configured({"foo": "bar"}, name="configured_solid")(None)
    assert result == 5

    result = solid_requires_config(get_solid_execution_context(solid_config={"foo": "bar"}))
    assert result == 5


def test_solid_with_inputs():
    @solid
    def solid_with_inputs(x, y):
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
    def solid_fails():
        raise Exception("Oh no!")

    with pytest.raises(
        DagsterSolidInvocationError,
        match='Error occurred while invoking solid "solid_fails":',
    ):
        solid_fails()


def test_attempted_invocation_in_composition():
    @solid
    def basic_solid(_x):
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
    async def aio_solid():
        await asyncio.sleep(0.01)
        return "done"

    assert aio_solid() == "done"


def test_async_gen_invocation():
    @solid
    async def aio_gen():
        await asyncio.sleep(0.01)
        yield Output("done")

    assert aio_gen() == "done"


def test_multiple_outputs_iterator():
    @solid
    def solid_multiple_outputs():
        yield Output(1, output_name="1")
        yield Output(2, output_name="2")

    assert solid_multiple_outputs() == {"1": 1, "2": 2}


@pytest.mark.parametrize(
    "property_or_method_name,val_to_pass",
    [
        ("pipeline_run", None),
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
        result(val_to_pass) if val_to_pass else result()  # pylint: disable=expression-not-assigned

    with pytest.raises(DagsterInvalidPropertyError):
        solid_fails_getting_property(None)


def test_allowed_properties_when_no_context_provided():
    @solid
    def solid_doesnt_use_resources(context):
        return context.resources

    with pytest.raises(DagsterInvalidPropertyError):
        solid_doesnt_use_resources(None)

    @solid
    def solid_doesnt_use_instance(context):
        return context.instance

    with pytest.raises(DagsterInvalidPropertyError):
        solid_doesnt_use_instance(None)
