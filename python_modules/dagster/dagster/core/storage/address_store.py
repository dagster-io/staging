from collections import namedtuple
from abc import ABCMeta, abstractmethod, abstractproperty
import os
from dagster.serdes import (
    serialize_dagster_namedtuple,
    deserialize_json_to_dagster_namedtuple,
    whitelist_for_serdes,
)
import six
import uuid
import pickle
from dagster import check
from dagster.core.definitions.events import AddressableAssetOperation
from dagster.core.errors import DagsterAddressIOError
from dagster.core.execution.context.system import SystemExecutionContext
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.types.dagster_type import DagsterType, resolve_dagster_type
from dagster.utils.backcompat import experimental
from dagster.utils import PICKLE_PROTOCOL
from .intermediate_storage import IntermediateStorage
from .object_store import (
    DEFAULT_SERIALIZATION_STRATEGY,
    FilesystemObjectStore,
    InMemoryObjectStore,
    ObjectStore,
)
from .type_storage import TypeStoragePluginRegistry


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


class AddressStore(IntermediateStorage):
    def __init__(self, asset_store, root_for_run_id, run_id, mapping=None):
        self.root_for_run_id = check.callable_param(root_for_run_id, "root_for_run_id")
        self.asset_store = asset_store
        self.mapping = mapping or {}

    # def _get_paths(self, step_output_handle):
    #     return ["aaa", step_output_handle.step_key, step_output_handle.output_name]

    def get_intermediate(self, context, dagster_type=None, step_output_handle=None):
        address = self.mapping.get(step_output_handle, None)
        if address is None:
            return  # address not found error
        return self.asset_store.get_asset(context, address)

    def set_intermediate(
        self,
        context,
        dagster_type=None,
        step_output_handle=None,
        value=None,
        version=None,
        path=None,
    ):
        address = self.asset_store.set_asset(context, value, path)  # TODO path
        self.mapping[step_output_handle] = address
        return AddressableAssetOperation(address, step_output_handle)

    def has_intermediate(self, context, step_output_handle):
        return self.mapping.get(step_output_handle) is not None

    def has_intermediate_at_address(self, address=None):
        raise NotImplementedError()

    def copy_intermediate_from_run(self, context, run_id, step_output_handle):
        raise NotImplementedError()

    @property
    def is_persistent(self):
        return True

    @property
    def root(self):
        return self.root_for_run_id(self.run_id)
