from typing import Mapping, NamedTuple, Sequence

from dagster.core.definitions.events import AssetKey
from dagster.core.definitions.i_solid_definition import NodeDefinition


class AssetNodeDefinition(NamedTuple):
    """A definition of a node in the logical asset graph.

    A function for computing the asset and an identifier for that asset.
    """

    key: AssetKey
    solid_def: NodeDefinition


class AssetDependencyDefinition(NamedTuple):
    """A definition of a directed edge in the logical asset graph.

    An upstream asset that's depended on, and the corresponding input name in the downstream asset
    that depends on it.
    """

    upstream_asset_key: AssetKey
    input_name: str


class AssetDefinitionGraph(NamedTuple):
    """A graph of asset definitions"""

    asset_defs: Sequence[AssetNodeDefinition]
    dependencies: Mapping[AssetKey, Sequence[AssetDependencyDefinition]]
