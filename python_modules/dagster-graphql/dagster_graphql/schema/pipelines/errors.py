import graphene
from dagster import check
from dagster.core.definitions.events import DagsterAssetKey

from ..errors import Error


class AssetsNotSupportedError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)


class AssetNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    def __init__(self, asset_key):
        super().__init__()
        self.asset_key = check.inst_param(asset_key, "asset_key", DagsterAssetKey)
        self.message = f"Asset key {asset_key.to_string()} not found."
