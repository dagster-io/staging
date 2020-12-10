from dagster.core.storage.asset_store import InMemoryAssetStore, mem_asset_store

mem_object_manager = mem_asset_store


class InMemoryBackcompatObjectManager(InMemoryAssetStore):
    # TODO: change it to InMemoryObjectManager
    """Adapter for deprecating Object Store and Serialization Strategy, in favor of Object Manager"""

    def has_object(self, context):
        keys = tuple(context.get_run_scoped_output_identifier())
        return keys in self.values
