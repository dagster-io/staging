import pytest
from dagster.core.decorator_utils import split_function_parameters
from dagster.core.errors import DagsterInvalidDefinitionError


def decorated_function_one_positional():
    def foo(bar):
        return bar

    return foo


def decorated_function_two_positionals_one_kwarg():
    def foo_kwarg(bar, baz, qux=True):
        return bar, baz, qux

    return foo_kwarg


def test_get_function_positional_parameters_ok():
    assert not split_function_parameters(decorated_function_one_positional(), ["bar"], lambda _: "")


def test_get_function_positional_parameters_multiple():
    non_positionals = split_function_parameters(
        decorated_function_two_positionals_one_kwarg(), ["bar", "baz"], lambda _: ""
    )
    assert {non_positional.name for non_positional in non_positionals} == {"qux"}


def test_get_function_positional_parameters_invalid():
    with pytest.raises(DagsterInvalidDefinitionError, match="foo"):
        split_function_parameters(decorated_function_one_positional(), ["bat"], lambda _: "foo")
