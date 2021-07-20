from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set

from dagster import InputDefinition, OutputDefinition, SolidDefinition
from dagster.core.decorator_utils import get_function_params, get_valid_name_permutations
from dagster.core.definitions.decorators.solid import _Solid


def asset(
    name: Optional[str] = None,
    namespace: Optional[List[str]] = None,
    input_namespaces: Optional[Mapping[str, Sequence]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None,
    required_resource_keys: Optional[Set[str]] = None,
) -> Callable[[Callable[..., Any]], SolidDefinition]:
    """Create a solid that updates an asset."""
    if callable(name):
        return _Asset()(name)

    if metadata and "namespace" in metadata:
        raise ValueError("TODO")

    def inner(fn: Callable[..., Any]) -> SolidDefinition:
        return _Asset(
            name=name,
            namespace=namespace,
            input_namespaces=input_namespaces,
            metadata=metadata,
            description=description,
            required_resource_keys=required_resource_keys,
        )(fn)

    return inner


class _Asset:
    def __init__(
        self,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        input_namespaces: Mapping[str, Sequence] = None,
        metadata: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
        required_resource_keys: Optional[Set[str]] = None,
    ):
        self.name = name
        self.namespace = namespace
        self.input_namespaces = input_namespaces or {}
        self.metadata = metadata
        self.description = description
        self.required_resource_keys = required_resource_keys

    def __call__(self, fn: Callable):
        asset_name = self.name or fn.__name__
        params = get_function_params(fn)
        is_context_provided = len(params) > 0 and params[0].name in get_valid_name_permutations(
            "context"
        )
        input_params = params[1:] if is_context_provided else params

        input_defs = [
            InputDefinition(
                name=input_param.name,
                metadata={
                    "logical_asset_namespace": self.input_namespaces.get(
                        input_param.name, self.namespace
                    ),
                    "logical_asset_name": input_param.name,
                },
                root_manager_key="root_manager",
            )
            for input_param in input_params
        ]
        output_def = OutputDefinition(
            metadata={"logical_asset_namespace": self.namespace, "logical_asset_name": asset_name}
        )
        return _Solid(
            name=asset_name,
            description=self.description,
            input_defs=input_defs,
            output_defs=[output_def],
            required_resource_keys=self.required_resource_keys,
        )(fn)
