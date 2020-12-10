from dagster.core.storage.asset_store import InMemoryAssetStore, mem_asset_store

mem_object_manager = mem_asset_store


class InMemoryBackcompatObjectManager(InMemoryAssetStore):
    # TODO: change it to InMemoryObjectManager
    """Adapter for deprecating Object Store and Serialization Strategy, in favor of Object Manager"""

    def handle_output(self, context, obj):
        self.set_asset(context, obj)

    def load_input(self, context):
        return self.get_asset(context)

    def has_object(self, context):
        keys = tuple(context.get_run_scoped_output_identifier())
        return keys in self.values
