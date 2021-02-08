from dagster.core.storage.io_manager import IOManager, io_manager
from dagster.core.storage.mem_asset_key_manager import InMemoryAssetKeyManager


class InMemoryIOManager(IOManager):
    def __init__(self):
        super(InMemoryIOManager, self).__init__()
        self.values = {}

    def handle_output(self, context, obj):
        keys = tuple(context.get_run_scoped_output_identifier())
        self.values[keys] = obj

    def load_input(self, context):
        keys = tuple(context.upstream_output.get_run_scoped_output_identifier())
        return self.values[keys]


class InMemoryAssetIOManager(InMemoryAssetKeyManager, InMemoryIOManager):
    pass


@io_manager
def mem_io_manager(_):
    """Built-in IO manager that stores and retrieves values in memory."""

    return InMemoryAssetIOManager()
