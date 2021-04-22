import asyncio

import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    Failure,
    Output,
    RetryRequested,
    pipeline,
    resource,
    solid,
)
from dagster.check import CheckError
from dagster.core.errors import (
    DagsterInvalidConfigError,
    DagsterInvalidDefinitionError,
    DagsterInvalidPropertyError,
    DagsterInvariantViolationError,
    DagsterSolidInvocationError,
)
from dagster.core.execution.context.invocation import build_solid_context


def test_solid_invocation_no_arg():
    @solid
    def basic_solid():
        return 5

    result = basic_solid()
    assert result == 5

    with pytest.raises(
        CheckError,
        match="Compute function of solid 'basic_solid' has no context "
        "argument, but context was provided when invoking.",
    ):
        basic_solid(build_solid_context())

    with pytest.raises(
        CheckError,
        match="Too many input arguments were provided for solid 'basic_solid'. This may be "
        "because an argument was provided for the context parameter, but no context parameter was "
        "defined for the solid.",
    ):
        basic_solid(None)


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
    context = build_solid_context()
    with pytest.raises(
        CheckError,
        match='Solid "solid_requires_resources" requires resource "foo", but no resource '
        "with that key was found on the context.",
    ):
        solid_requires_resources(context)

    context = build_solid_context(resources={"foo": "bar"})
    assert solid_requires_resources(context) == "bar"


def test_solid_invocation_with_cm_resource():
    teardown_log = []

    @resource
    def cm_resource(_):
        try:
            yield "foo"
        finally:
            teardown_log.append("collected")

    @solid(required_resource_keys={"cm_resource"})
    def solid_requires_cm_resource(context):
        return context.resources.cm_resource

    # Attempt to use solid context as fxn with cm resource should fail
    context = build_solid_context(resources={"cm_resource": cm_resource})
    with pytest.raises(DagsterInvariantViolationError):
        solid_requires_cm_resource(context)

    del context
    assert teardown_log == ["collected"]

    # Attempt to use solid context as cm with cm resource should succeed
    with build_solid_context(resources={"cm_resource": cm_resource}) as context:
        assert solid_requires_cm_resource(context) == "foo"

    assert teardown_log == ["collected", "collected"]


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
    context = build_solid_context()
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

    result = solid_requires_config(build_solid_context(config={"foo": "bar"}))
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

    output_one, output_two = solid_multiple_outputs()
    assert output_one == 1
    assert output_two == 2


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


def test_solid_retry_requested():
    @solid
    def solid_retries():
        raise RetryRequested()

    with pytest.raises(
        DagsterInvariantViolationError,
        match="Retry not requested. Retries are not supported when directly invoking solids.",
    ):
        solid_retries()


def test_solid_failure():
    @solid
    def solid_fails():
        raise Failure("oops")

    with pytest.raises(DagsterSolidInvocationError):
        solid_fails()


def test_yielded_asset_materialization():
    @solid
    def solid_yields_materialization(_):
        yield AssetMaterialization(asset_key=AssetKey(["fake"]))
        yield Output(5)
        yield AssetMaterialization(asset_key=AssetKey(["fake2"]))

    # Ensure that running without context works, and that asset
    # materializations are just ignored in this case.
    assert solid_yields_materialization(None) == 5

    context = build_solid_context()
    assert solid_yields_materialization(context) == 5
    materializations = context.asset_materializations
    assert len(materializations) == 2
