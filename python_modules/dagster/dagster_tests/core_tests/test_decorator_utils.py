from dagster.core.decorator_utils import get_function_params, missing_required_positionals


def decorated_function_one_positional():
    def foo(bar):
        return bar

    return foo


def decorated_function_two_positionals_one_kwarg():
    def foo_kwarg(bar, baz, qux=True):
        return bar, baz, qux

    return foo_kwarg


def test_one_required_positional_param():
    positionals = ["bar"]
    assert not missing_required_positionals(decorated_function_one_positional(), positionals)
    assert not get_function_params(decorated_function_one_positional(), positionals)


def test_required_positional_parameters_not_missing():
    positionals = ["bar", "baz"]
    assert not missing_required_positionals(
        decorated_function_two_positionals_one_kwarg(), positionals
    )
    non_required_args = get_function_params(
        decorated_function_two_positionals_one_kwarg(), positionals
    )

    assert {non_required_arg.name for non_required_arg in non_required_args} == {"qux"}

    assert missing_required_positionals(decorated_function_one_positional(), positionals) == "baz"
