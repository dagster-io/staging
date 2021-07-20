from typing import AbstractSet, Any, Dict, List, Mapping, Optional, Tuple, Union, cast

from dagster import (
    DependencyDefinition,
    InputContext,
    PipelineDefinition,
    ResourceDefinition,
    RootInputManagerDefinition,
    SolidDefinition,
    build_input_context,
    build_output_context,
    root_input_manager,
)
from dagster.core.definitions.dependency import IDependencyDefinition, SolidInvocation
from dagster.core.definitions.graph import GraphDefinition
from dagster.core.definitions.i_solid_definition import NodeDefinition
from dagster.utils.merger import merge_dicts

from .source_asset import SourceAsset


def build_assets_job(
    name: str,
    assets: List[NodeDefinition],
    source_assets: Optional[List[SourceAsset]] = None,
    resource_defs: Dict[str, ResourceDefinition] = None,
    description: Optional[str] = None,
) -> PipelineDefinition:
    """Builds a pipeline that refreshes the given assets."""
    source_assets_by_path = {
        (source.namespace, source.name): source for source in source_assets or {}
    }

    solid_deps = build_solid_deps(assets, source_assets_by_path.keys())
    root_manager = build_root_manager(source_assets_by_path)

    return GraphDefinition(
        name=name,
        node_defs=assets,
        dependencies=solid_deps,
        description=description,
        input_mappings=None,
        output_mappings=None,
        config_mapping=None,
    ).to_job(resource_defs=merge_dicts(resource_defs or {}, {"root_manager": root_manager}))


def build_solid_deps(
    assets: List[NodeDefinition], source_paths: AbstractSet[Tuple[str]]
) -> Dict[Union[str, SolidInvocation], Dict[str, IDependencyDefinition]]:
    solids_by_logical_asset: Dict[str, SolidDefinition] = {}
    for solid in assets:
        n_outputs = len(solid.output_defs)
        if len(solid.output_defs) != 1:
            raise ValueError(
                f"Solids must have exactly one output, but '{solid.name}'' has {n_outputs}"
            )

        output_def = solid.output_defs[0]
        logical_asset = get_asset_path(
            output_def.metadata, f"Output metadata of solid '{solid.name}'"
        )

        if logical_asset in solids_by_logical_asset:
            prev_solid = solids_by_logical_asset[logical_asset].name
            raise ValueError(
                f"Two solids produce the same logical asset: '{solid.name}' and '{prev_solid.name}"
            )

        solids_by_logical_asset[logical_asset] = solid

    solid_deps: Dict[Union[str, SolidInvocation], Dict[str, IDependencyDefinition]] = {}
    for solid in assets:
        solid_deps[solid.name] = {}
        for input_def in solid.input_defs:
            logical_asset = get_asset_path(
                input_def.metadata,
                f"Metadata for input '{input_def.name}' of solid '{solid.name}'",
            )

            if logical_asset in solids_by_logical_asset:
                solid_deps[solid.name][input_def.name] = DependencyDefinition(
                    solids_by_logical_asset[logical_asset].name, "result"
                )
            elif logical_asset not in source_paths:
                raise ValueError(
                    f"Logical input asset '{logical_asset}' for solid '{solid.name}' is not "
                    "produced by any of the provided asset solids and is not one of the provided "
                    "sources"
                )

    return solid_deps


def build_root_manager(
    source_assets_by_path: Mapping[str, SourceAsset]
) -> RootInputManagerDefinition:
    @root_input_manager(required_resource_keys={"io_manager"})
    def _root_manager(input_context: InputContext) -> Any:
        source_asset_path = get_asset_path(input_context.metadata, "Metadata for input")
        source_asset = source_assets_by_path[source_asset_path]

        output_context = build_output_context(
            name=source_asset.name, step_key="none", metadata=source_asset.metadata
        )
        input_context_with_upstream = build_input_context(
            name=input_context.name, upstream_output=output_context
        )

        return cast(Any, input_context.resources).io_manager.load_input(input_context_with_upstream)

    return _root_manager


def get_asset_path(metadata: Optional[Mapping[str, Any]], error_prefix: str) -> Tuple[str, str]:
    if metadata is None:
        raise ValueError(f"{error_prefix}' is None")

    if "logical_asset_name" not in metadata:
        raise ValueError(f"{error_prefix} is missing 'logical_asset_name'")

    if "logical_asset_namespace" not in metadata:
        raise ValueError(f"{error_prefix} is missing 'logical_asset_namespace'")

    return (
        metadata["logical_asset_namespace"],
        metadata["logical_asset_name"],
    )
