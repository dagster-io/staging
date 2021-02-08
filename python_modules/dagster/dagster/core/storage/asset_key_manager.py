from abc import ABC, abstractmethod
from typing import List

from dagster import AssetKey


class AssetKeyManager(ABC):
    """
    Base interface for classes that are responsible for loading solid inputs.
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
