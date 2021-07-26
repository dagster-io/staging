from typing import Mapping, NamedTuple, Sequence

from dagster.core.definitions.events import AssetKey
from dagster.core.definitions.i_solid_definition import NodeDefinition


class AssetDefinition(NamedTuple):
    key: AssetKey
    solid_def: NodeDefinition


class AssetDependencyDefinition(NamedTuple):
    upstream_asset_key: AssetKey
    input_name: str


class AssetDefinitionGraph(NamedTuple):
    """A graph of asset definitions"""

    asset_defs: Sequence[AssetDefinition]
    dependencies: Mapping[AssetKey, Sequence[AssetDependencyDefinition]]
