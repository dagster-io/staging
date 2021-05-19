import pytest
from dagster import InitResourceContext, resource
from dagster.core.errors import DagsterInvariantViolationError
from dagster.experimental import build_resource_context


def test_build_no_args():
    assert isinstance(build_resource_context(), InitResourceContext)


def test_build_with_resources():
    @resource
    def foo(_):
        return "foo"

    context = build_resource_context(resources={"foo": foo, "bar": "bar"})
    assert context.resources.foo == "foo"
    assert context.resources.bar == "bar"


def test_build_with_cm_resource():
    entered = []

    @resource
    def foo(_):
        try:
            yield "foo"
        finally:
            entered.append("true")

    context = build_resource_context(resources={"foo": foo})
    with pytest.raises(DagsterInvariantViolationError):
        context.resources  # pylint: disable=pointless-statement

    del context
    assert entered == ["true"]

    with build_resource_context(resources={"foo": foo}) as context:
        assert context.resources.foo == "foo"

    assert entered == ["true", "true"]
