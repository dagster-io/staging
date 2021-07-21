from typing import AbstractSet, Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union, cast

from dagster import (
    DependencyDefinition,
    InputContext,
    PipelineDefinition,
    ResourceDefinition,
    RootInputManagerDefinition,
    build_input_context,
    build_output_context,
    check,
    root_input_manager,
)
from dagster.core.definitions.dependency import IDependencyDefinition, SolidInvocation
from dagster.core.definitions.events import AssetKey
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
    nodes_after: Sequence[Tuple[NodeDefinition, Mapping[str, AssetKey]]] = None,
) -> PipelineDefinition:
    """Builds a job that refreshes the given assets."""
    check.str_param(name, "name")
    check.list_param(assets, "assets", of_type=NodeDefinition)
    check.opt_list_param(source_assets, "source_assets", of_type=SourceAsset)
    check.opt_str_param(description, "description")
    check.opt_list_param(nodes_after, "nodes_after", of_type=tuple)

    source_assets_by_key = {source.key: source for source in source_assets or {}}

    solid_deps = build_solid_deps(assets, source_assets_by_key.keys(), nodes_after)
    root_manager = build_root_manager(source_assets_by_key)

    return GraphDefinition(
        name=name,
        node_defs=assets + [node for node, _ in nodes_after or []],
        dependencies=solid_deps,
        description=description,
        input_mappings=None,
        output_mappings=None,
        config_mapping=None,
    ).to_job(resource_defs=merge_dicts(resource_defs or {}, {"root_manager": root_manager}))


def build_solid_deps(
    assets: List[NodeDefinition],
    source_paths: AbstractSet[Tuple[str]],
    nodes_after: Sequence[Tuple[NodeDefinition, Mapping[str, AssetKey]]],
) -> Dict[Union[str, SolidInvocation], Dict[str, IDependencyDefinition]]:
    solids_by_logical_asset: Dict[AssetKey, NodeDefinition] = {}
    for solid in assets:
        n_outputs = len(solid.output_defs)
        if len(solid.output_defs) != 1:
            raise ValueError(
                f"Solids must have exactly one output, but '{solid.name}'' has {n_outputs}"
            )

        output_def = solid.output_defs[0]
        logical_asset = get_asset_key(
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
            logical_asset = get_asset_key(
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

    for node_def, deps in nodes_after or []:
        check.is_dict(deps, key_type=str, value_type=AssetKey)
        solid_deps[node_def.name] = {}
        for input_name, asset_key in deps.items():
            solid_deps[node_def.name][input_name] = DependencyDefinition(
                solids_by_logical_asset[asset_key].name, "result"
            )

    return solid_deps


def build_root_manager(
    source_assets_by_key: Mapping[AssetKey, SourceAsset]
) -> RootInputManagerDefinition:
    @root_input_manager(required_resource_keys={"io_manager"})
    def _root_manager(input_context: InputContext) -> Any:
        source_asset_path = get_asset_key(input_context.metadata, "Metadata for input")
        source_asset = source_assets_by_key[source_asset_path]

        output_context = build_output_context(
            name=source_asset.key.path[-1], step_key="none", metadata=source_asset.metadata
        )
        input_context_with_upstream = build_input_context(
            name=input_context.name, upstream_output=output_context
        )

        return cast(Any, input_context.resources).io_manager.load_input(input_context_with_upstream)

    return _root_manager


def get_asset_key(metadata: Optional[Mapping[str, Any]], error_prefix: str) -> AssetKey:
    if metadata is None:
        raise ValueError(f"{error_prefix}' is None")

    if "logical_asset_key" not in metadata:
        raise ValueError(f"{error_prefix} is missing 'logical_asset_key'")

    return metadata["logical_asset_key"]
