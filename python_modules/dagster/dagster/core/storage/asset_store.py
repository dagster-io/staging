import os
import pickle
import uuid
from collections import namedtuple

from dagster import check
from dagster.serdes import (
    deserialize_json_to_dagster_namedtuple,
    serialize_dagster_namedtuple,
    whitelist_for_serdes,
)
from dagster.utils import PICKLE_PROTOCOL


class AssetAddress:
    """
    Pointer to an addressable asset.
    it contains the metadata of an addressable assets
    """


class AssetStore:
    """
    - handle write and read user-defined function
    - create AssetAddress for addressale asset tracking
    """


@whitelist_for_serdes
class PickledObjectFileystemAssetAddress(
    namedtuple("_PickledObjectFileystemAssetAddress", "asset_id path"), AssetAddress
):
    def __new__(
        cls, asset_id, path,
    ):
        return super(PickledObjectFileystemAssetAddress, cls).__new__(
            cls, asset_id=check.str_param(asset_id, "asset_id"), path=check.str_param(path, "path")
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

    def store_to_file(self, value, write_path):
        # built in `store_to_file` - will be user-defined code
        check.str_param(write_path, "write_path")
        with open(write_path, self.write_mode) as write_obj:
            pickle.dump(value, write_obj, PICKLE_PROTOCOL)

    def load_from_file(self, read_path):
        # built in `load_from_file` - will be user-defined code
        check.str_param(read_path, "read_path")

        with open(read_path, self.read_mode) as read_obj:
            return pickle.load(read_obj)

    def _get_path(self, path):
        return os.path.join(self.base_dir, path)

    def _get_new_id(self):
        return str(uuid.uuid4())

    def set_asset(self, _context, obj, path):
        """
        store data object to file and track it as AddressablAsset
        """
        address_path = self._get_path(path)
        self.store_to_file(obj, address_path)

        return PickledObjectFileystemAssetAddress(self._get_new_id(), address_path)

    def get_asset(self, _context, address):
        """
        load data object from file using AssetAddress
        """
        return self.load_from_file(address.path)
