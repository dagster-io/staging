import pytest
from dagster import build_resource_init_context, resource
from dagster.core.errors import DagsterInvalidConfigError, DagsterInvalidInvocationError


def test_resource_invocation_none_arg():
    @resource
    def basic_resource(_):
        return 5

    assert basic_resource(None) == 5

    with pytest.raises(
        DagsterInvalidInvocationError,
        match="Compute function of resources have context argument, but no "
        "context was provided when invoking.",
    ):
        basic_resource()  # pylint: disable=no-value-for-parameter


def test_resource_invocation_with_resources():
    @resource(required_resource_keys={"foo"})
    def resource_reqs_resources(init_context):
        return init_context.resources.foo

    with pytest.raises(
        DagsterInvalidInvocationError,
        match="Resource has required resources, but no context was " "provided.",
    ):
        resource_reqs_resources(None)

    context = build_resource_init_context()

    with pytest.raises(
        DagsterInvalidInvocationError,
        match='Resource requires resource "foo", but no resource '
        "with that key was found on the context.",
    ):
        resource_reqs_resources(context)

    context = build_resource_init_context(resources={"foo": "bar"})
    assert resource_reqs_resources(context) == "bar"


def test_resource_invocation_with_cm_resource():
    teardown_log = []

    @resource
    def cm_resource(_):
        try:
            yield "foo"
        finally:
            teardown_log.append("collected")

    with cm_resource(None) as resource_val:  # pylint: disable=not-context-manager
        assert resource_val == "foo"
        assert not teardown_log

    assert teardown_log == ["collected"]


def test_resource_invocation_with_config():
    @resource(config_schema={"foo": str})
    def resource_reqs_config(context):
        assert context.resource_config["foo"] == "bar"
        return 5

    # Ensure that error is raised when we attempt to invoke with a None context
    with pytest.raises(
        DagsterInvalidInvocationError,
        match="Resource has required config schema, but no context was " "provided.",
    ):
        resource_reqs_config(None)

    # Ensure that error is raised when context does not have the required config.
    context = build_resource_init_context()
    with pytest.raises(
        DagsterInvalidConfigError,
        match="Error in config for resource",
    ):
        resource_reqs_config(context)

    # Ensure that if you configure the respirce, you can provide a none-context.
    result = resource_reqs_config.configured({"foo": "bar"})(None)
    assert result == 5

    result = resource_reqs_config(build_resource_init_context(config={"foo": "bar"}))
    assert result == 5


def test_failing_resource():
    @resource
    def fails(_):
        raise Exception("Oh no!")

    with pytest.raises(Exception, match="Oh no!"):
        fails(None)
