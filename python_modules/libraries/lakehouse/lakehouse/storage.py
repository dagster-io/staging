from abc import ABCMeta, abstractmethod


class AssetStorage(metaclass=ABCMeta):
    """An AssetStorage describes how to save and load assets."""

    @abstractmethod
    def save(self, obj, path, resources):
        pass

    @abstractmethod
    def load(self, python_type, path, resources):
        pass
