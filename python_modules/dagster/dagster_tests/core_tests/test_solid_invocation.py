from dagster import resource, solid
from dagster.core.execution.context.compute import build_solid_execution_context


def test_solid_invocation():
    @solid
    def basic_solid(_):
        return 5

    result = basic_solid(None)
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

    with build_solid_execution_context(
        resources={"foo": "from_instance", "baz": baz_resource}
    ) as context:
        result = solid_requires_resources(context)
        assert result == 5


def test_solid_invocation_with_config():
    @solid(config_schema={"foo": str})
    def solid_requires_config(context):
        assert context.solid_config["foo"] == "bar"
        return 5

    with build_solid_execution_context(solid_config={"foo": "bar"}) as context:
        result = solid_requires_config(context)
        assert result == 5


def test_solid_with_inputs():
    @solid
    def solid_with_inputs(_, x):
        assert x == 5
        return x + 1

    assert solid_with_inputs(None, 5) == 6
