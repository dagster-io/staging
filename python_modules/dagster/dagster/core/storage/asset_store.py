import os
import pickle
import uuid
from abc import ABCMeta, abstractmethod
from collections import namedtuple

import six

from dagster import check
from dagster.config import Field
from dagster.config.source import StringSource
from dagster.core.definitions.resource import resource
from dagster.serdes import (
    deserialize_json_to_dagster_namedtuple,
    serialize_dagster_namedtuple,
    whitelist_for_serdes,
)
from dagster.utils import PICKLE_PROTOCOL


class AssetStoreHandle(namedtuple("_AssetStoreHandle", "asset_store_key asset_metadata")):
    def __new__(cls, asset_store_key, asset_metadata=None):
        return super(AssetStoreHandle, cls).__new__(
            cls,
            asset_store_key=check.str_param(asset_store_key, "asset_store_key"),
            asset_metadata=check.opt_dict_param(asset_metadata, "asset_metadata", key_type=str),
        )


class AssetAddress:
    """
    Pointer to an addressable asset.
    it contains the metadata of an addressable assets
    """


class AssetStore(six.with_metaclass(ABCMeta)):  # pylint: disable=no-init
    """
    - handle write and read user-defined function
    - create AssetAddress for addressale asset tracking
    """

    @abstractmethod
    def set_asset(self, context, obj, asset_metadata):
        pass

    @abstractmethod
    def get_asset(self, context, address):
        pass


@whitelist_for_serdes
class PickledObjectFileystemAssetAddress(
    namedtuple("_PickledObjectFileystemAssetAddress", "asset_id filepath"), AssetAddress
):
    def __new__(
        cls, asset_id, filepath,
    ):
        return super(PickledObjectFileystemAssetAddress, cls).__new__(
            cls,
            asset_id=check.str_param(asset_id, "asset_id"),
            filepath=check.str_param(filepath, "filepath"),
        )

    def to_string(self):
        return serialize_dagster_namedtuple(self)

    @staticmethod
    def from_string(json_str):
        return deserialize_json_to_dagster_namedtuple(json_str)


class PickledObjectFileystemAssetStore(AssetStore):
    def __init__(self, base_dir=None):
        self.base_dir = check.opt_str_param(base_dir, "base_dir")
        self.write_mode = "wb"
        self.read_mode = "rb"

    def _get_path(self, path):
        return os.path.join(self.base_dir, path)

    def _get_new_id(self):
        return str(uuid.uuid4())

    def set_asset(self, _context, obj, asset_metadata):
        """
        store data object to file and track it as AddressablAsset
        """
        check.dict_param(asset_metadata, "asset_metadata", key_type=str)
        path = check.str_param(asset_metadata.get("path"), "asset_metadata.path")
        filepath = self._get_path(path)

        with open(filepath, self.write_mode) as write_obj:
            pickle.dump(obj, write_obj, PICKLE_PROTOCOL)

        return PickledObjectFileystemAssetAddress(asset_id=self._get_new_id(), filepath=filepath)

    def get_asset(self, _context, address):
        """
        load data object from file using AssetAddress
        """
        filepath = check.str_param(address.filepath, "filepath")

        with open(filepath, self.read_mode) as read_obj:
            return pickle.load(read_obj)


@resource(config_schema={"base_dir": Field(StringSource, default_value=".", is_required=False)})
def default_filesystem_asset_store(init_context):
    return PickledObjectFileystemAssetStore(init_context.resource_config["base_dir"])
