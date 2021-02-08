from dagster.core.definitions import AssetOutput
from dagster.core.storage.asset_key_manager import AssetKeyManager


class InMemoryAssetKeyManager(AssetKeyManager):
    def __init__(self):
        super(InMemoryAssetKeyManager, self).__init__()
        self.asset_keys = {}

    def handle_output_asset_keys(self, context, output):
        keys = tuple(context.get_run_scoped_output_identifier())
        asset_keys = []
        if isinstance(output, AssetOutput):
            asset_keys = [m.asset_key for m in output.asset_materializations]
        self.asset_keys[keys] = asset_keys

    def load_input_asset_keys(self, context):
        keys = tuple(context.upstream_output.get_run_scoped_output_identifier())
        return self.asset_keys[keys]
