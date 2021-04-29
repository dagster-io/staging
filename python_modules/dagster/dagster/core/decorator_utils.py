from typing import Any, Callable, List, Optional

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


def missing_required_positionals(
    fn: Callable[..., Any], expected_positionals: List[str]
) -> Optional[str]:
    """Returns first missing required positional, if any, otherwise None"""
    fn_params = list(funcsigs.signature(fn).parameters.values())
    expected_idx = 0
    for expected_positional in expected_positionals:
        if expected_idx >= len(fn_params) or not _is_param_valid(
            fn_params[expected_idx], expected_positional
        ):
            return expected_positional
        expected_idx += 1
    return None


def get_function_params(fn: Callable[..., Any], positionals: List[str]) -> List[funcsigs.Parameter]:
    """Returns the function params after the provided positionals.

    Assumes that positionals have already been checked for existence if they are required, and thus
    will not fail if they are not found.
    """
    fn_params = list(funcsigs.signature(fn).parameters.values())
    # params_skipped is true if the provided positionals are present, false if not
    positionals_provided = all(
        [
            _is_param_valid(fn_param, positional)
            for fn_param, positional in zip(fn_params, positionals)
        ]
    )

    start_idx = len(positionals) if positionals_provided else 0
    return fn_params[start_idx:]


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
