from dagster.core.definitions.events import AssetKey
from dagster.core.storage.io_manager import IOManager


class AssetIOManager(IOManager):
    def get_asset_key(self, context) -> AssetKey:
        raise NotImplementedError()
