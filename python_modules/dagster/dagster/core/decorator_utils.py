from typing import Any, Callable, List

from dagster.core.errors import DagsterInvalidDefinitionError
from dagster.seven import funcsigs


def _is_param_valid(param: funcsigs.Parameter, expected_positional: str) -> bool:
    # The "*" character indicates that we permit any name for this positional parameter.
    if expected_positional == "*":
        return True

    possible_names = {
        "_",
        expected_positional,
        f"_{expected_positional}",
        f"{expected_positional}_",
    }
    possible_kinds = {funcsigs.Parameter.POSITIONAL_OR_KEYWORD, funcsigs.Parameter.POSITIONAL_ONLY}

    return param.name in possible_names and param.kind in possible_kinds


def split_function_parameters(
    fn: Callable[..., Any], expected_positionals: List[str], error_msg_lambda: Callable[[str], str]
) -> List[funcsigs.Parameter]:
    """Validate the parameters of `fn` against an expected list of positional arguments.

    Args:
        fn (Callable[..., Any]): The function whose arguments we want to validate
        expected_positionals (List[str]): A list of argument names that we expect to be at the front
            of the argument list.
        error_msg_lambda (Callable[[str], str]): A function that takes in the name of the missing
            positional, and returns an error message specific to the callsite.

    Returns:
        List[funcsigs.Parameter]: A list of arguments in `fn` after the expected positional
            arguments
    """

    fn_params = list(funcsigs.signature(fn).parameters.values())
    if len(fn_params) < len(expected_positionals):
        raise DagsterInvalidDefinitionError(error_msg_lambda(expected_positionals[0]))

    for fn_param, expected_positional in zip(fn_params, expected_positionals):
        if not _is_param_valid(fn_param, expected_positional):
            raise DagsterInvalidDefinitionError(error_msg_lambda(expected_positional))

    return fn_params[len(expected_positionals) :]


def is_required_param(param):
    return param.default == funcsigs.Parameter.empty


def positional_arg_name_list(params):
    return list(
        map(
            lambda p: p.name,
            filter(
                lambda p: p.kind
                in [funcsigs.Parameter.POSITIONAL_OR_KEYWORD, funcsigs.Parameter.POSITIONAL_ONLY],
                params,
            ),
        )
    )
