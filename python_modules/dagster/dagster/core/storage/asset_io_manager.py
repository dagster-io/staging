from abc import abstractmethod
from typing import List

from dagster.core.definitions import AssetKey
from dagster.core.storage.asset_key_manager import AssetKeyManager
from dagster.core.storage.io_manager import IOManager


class AssetIOManager(IOManager, AssetKeyManager):
    @abstractmethod
    def load_input(self, context):
        """User-defined method that loads an input to a solid.

        Args:
            context (InputContext): The input context, which describes the input that's being loaded
                and the upstream output that's being loaded from.

        Returns:
            Any: The data object.
        """

    @abstractmethod
    def handle_output(self, context, obj):
        """User-defined method that stores an output of a solid.

        Args:
            context (OutputContext): The context of the step output that produces this object.
            obj (Any): The object, returned by the solid, to be stored.
        """

    @abstractmethod
    def handle_output_asset_keys(self, context, output):
        """
        TODO
        """

    @abstractmethod
    def load_input_asset_keys(self, context) -> List[AssetKey]:
        """
        TODO
        """
