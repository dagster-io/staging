import pytest
from dagster import resource, solid
from dagster.check import CheckError


def test_solid_invocation():
    @solid
    def basic_solid(_):
        return 5

    result = basic_solid()
    assert result == 5


def test_solid_invocation_with_resources():
    @solid(required_resource_keys={"foo", "baz"})
    def solid_requires_resources(context):
        assert context.resources.foo == "from_instance"
        assert context.resources.baz == "from_def"
        return 5

    @resource
    def baz_resource(_):
        return "from_def"

    # Ensure that a proper error is thrown when attempting to execute and no context is provided
    with pytest.raises(
        CheckError,
        match='Solid "solid_requires_resources" has required resources, but no resources have been '
        "provided.",
    ):
        solid_requires_resources()

    result = solid_requires_resources.invokable(
        resources={"foo": "from_instance", "baz": baz_resource}
    )()
    assert result == 5


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

    result = solid_requires_config.invokable(solid_config={"foo": "bar"})()
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

    # Check for proper error when keyword args are provided, and they are out-of-order
    with pytest.raises(
        CheckError, match='Keyword argument "y" provided at position of argument "x".'
    ):
        solid_with_inputs(y=5, x=6)

    # Check for proper error when incorrect number of inputs is provided.
    with pytest.raises(
        CheckError, match='Solid "solid_with_inputs" expected 2 inputs, but 1 were provided.'
    ):
        solid_with_inputs(5)
