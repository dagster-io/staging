from typing import Any, Dict, List, Optional

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
from dagster.core.definitions.graph import GraphDefinition
from dagster.utils.merger import merge_dicts

from .source_asset import SourceAsset


def build_assets_job(
    name: str,
    asset_solids: List[SolidDefinition],
    source_assets: Optional[List[SourceAsset]] = None,
    resource_defs: Dict[str, ResourceDefinition] = None,
    description: Optional[str] = None,
) -> PipelineDefinition:
    """Builds a pipeline that refreshes the given assets."""
    source_paths = {source.path for source in source_assets or []}

    solids_by_logical_asset: Dict[str, SolidDefinition] = {}
    for solid in asset_solids:
        n_outputs = len(solid.output_defs)
        if len(solid.output_defs) != 1:
            raise ValueError(
                f"Solids must have exactly one output, but '{solid.name}'' has {n_outputs}"
            )

        output_def = solid.output_defs[0]
        if output_def.metadata is None or "logical_asset" not in output_def.metadata:
            raise ValueError(f"Output metadata of solid '{solid.name}' is missing 'logical_asset'")

        logical_asset = output_def.metadata["logical_asset"]
        if logical_asset in solids_by_logical_asset:
            prev_solid = solids_by_logical_asset[logical_asset].name
            raise ValueError(
                f"Two solids produce the same logical asset: '{solid.name}' and '{prev_solid.name}"
            )

        solids_by_logical_asset[logical_asset] = solid

    solid_deps: Dict[str, Dict[str, DependencyDefinition]] = {}
    for solid in asset_solids:
        solid_deps[solid.name] = {}
        for input_def in solid.input_defs:
            if input_def.metadata is None or "logical_asset" not in input_def.metadata:
                raise ValueError(
                    f"Metadata for input '{input_def.name}'' of solid '{solid.name}' is missing "
                    "'logical_asset'"
                )

            logical_asset = input_def.metadata["logical_asset"]
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

    root_manager = build_root_manager(source_assets or [])

    return GraphDefinition(
        name=name,
        node_defs=asset_solids,
        dependencies=solid_deps,
        description=description,
        input_mappings=None,
        output_mappings=None,
        config_mapping=None,
    ).to_job(resource_defs=merge_dicts(resource_defs or {}, {"root_manager": root_manager}))


def build_root_manager(source_assets: List[SourceAsset]) -> RootInputManagerDefinition:
    source_assets_by_path = {source_asset.path: source_asset for source_asset in source_assets}

    @root_input_manager(required_resource_keys={"io_manager"})
    def _root_manager(input_context: InputContext) -> Any:
        source_asset = source_assets_by_path[input_context.metadata["logical_asset"]]

        output_context = build_output_context(
            name=source_asset.path[-1], step_key="none", metadata=source_asset.metadata
        )
        input_context_with_upstream = build_input_context(
            name=input_context.name, upstream_output=output_context
        )

        io_manager = input_context.resources.io_manager
        return io_manager.load_input(input_context_with_upstream)

    return _root_manager
