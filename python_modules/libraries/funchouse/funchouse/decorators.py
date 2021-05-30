from typing import AbstractSet, Any, Callable, Dict, List, Optional, Tuple, Union

from dagster import InputDefinition, OutputDefinition, SolidDefinition
from dagster.core.decorator_utils import get_function_params, get_valid_name_permutations
from dagster.core.definitions.decorators.solid import _Solid


def asset(
    name: Optional[str] = None,
    namespace: Optional[List[str]] = None,
    inputs: Optional[List[Union[Tuple[str], List[str]]]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None,
    required_resource_keys: Optional[AbstractSet[str]] = None,
) -> Callable[[Callable[..., Any]], SolidDefinition]:
    """Create a solid that updates an asset."""
    if metadata and "namespace" in metadata:
        raise ValueError("TODO")

    def inner(fn: Callable[..., Any]) -> SolidDefinition:
        asset_name = name or fn.__name__
        params = get_function_params(fn)
        is_context_provided = len(params) > 0 and params[0].name in get_valid_name_permutations(
            "context"
        )
        input_params = params[1:] if is_context_provided else params
        if len(input_params) != len(inputs or []):
            raise ValueError("TODO")

        input_defs = [
            InputDefinition(
                name=input_param.name,
                metadata={"logical_asset": tuple(input_path)},
                root_manager_key="root_manager",
            )
            for input_path, input_param in zip(inputs or [], input_params)
        ]
        output_def = OutputDefinition(
            metadata={"logical_asset": tuple(namespace or ()) + (asset_name,)}
        )
        return _Solid(
            name=asset_name,
            description=description,
            input_defs=input_defs,
            output_defs=[output_def],
            required_resource_keys=required_resource_keys,
        )(fn)

    return inner
