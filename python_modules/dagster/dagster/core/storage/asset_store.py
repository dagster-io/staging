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
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.serdes import (
    deserialize_json_to_dagster_namedtuple,
    serialize_dagster_namedtuple,
    whitelist_for_serdes,
)
from dagster.utils import PICKLE_PROTOCOL, mkdir_p


@whitelist_for_serdes
class AssetStoreHandle(namedtuple("_AssetStoreHandle", "asset_store_key asset_metadata")):
    def __new__(cls, asset_store_key, asset_metadata=None):
        return super(AssetStoreHandle, cls).__new__(
            cls,
            asset_store_key=check.str_param(asset_store_key, "asset_store_key"),
            asset_metadata=check.opt_dict_param(asset_metadata, "asset_metadata", key_type=str),
        )


class AssetAddress:
    """
    Base class for addressable asset pointer.

    Extend this class to represent the metadata of an addressable asset paried to a user-provided
    subclass of :py:class:`~dagster.AssetStore`.
    """


class AssetStore(six.with_metaclass(ABCMeta)):  # pylint: disable=no-init
    """
    Base class for user-provided asset store.

    Extend this class to handle asset operations. Users should implement ``set_asset`` to write a
    data object that can be tracked by the Dagster machinery and ``get_asset`` to read an existing
    data object.
    """

    @abstractmethod
    def set_asset(self, context, step_output_handle, obj, asset_metadata):
        """The user-definied write method that stores a data object.

        Note: this method should return a subclass of :py:class:`~dagster.AssetAddress` for the
        Dagster machinery to track the stored data object, i.e. an asset. The scheme of the asset
        address typically should line up with the user-provided asset store.

        Args:
            context (SystemStepExecutionContext): The context that the corresponding step is in.
            step_output_handle (StepOutputHandle): the handle of the step output which generates
                the asset.
            obj (Any): The data object to be stored.
            asset_metadata (Dict[str, Any]): The metadata defined on the step that produced the
                given data object. For example, users can provide a file path if the data object
                will be stored in a filesystem, or provide information of a database table when it
                is going to load the data into the table.


        Returns:
            AssetAddress: a pointer to the stored data.
        """

    @abstractmethod
    def get_asset(self, context, step_output_handle, asset_metadata):
        """The user-defined read method that loads data given a tracked address.

        Args:
            context (SystemStepExecutionContext): The context that the corresponding step is in.
            step_output_handle (StepOutputHandle): the handle of the step output which generates
                the asset.
            asset_metadata (Dict[str, Any]): The metadata defined on the step that produced the
                data object to get.

        Returns:
            Any: The data object.
        """


@whitelist_for_serdes
class PickledObjectFileystemAssetAddress(
    namedtuple("_PickledObjectFileystemAssetAddress", "asset_id asset_metadata"), AssetAddress
):
    def __new__(cls, asset_id, asset_metadata):
        return super(PickledObjectFileystemAssetAddress, cls).__new__(
            cls,
            asset_id=check.str_param(asset_id, "asset_id"),
            asset_metadata=check.opt_dict_param(asset_metadata, "asset_metadata", key_type=str),
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

    def _get_path(self, step_output_handle, path):
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)
        check.opt_str_param(path, "path")
        if path:
            return os.path.join(self.base_dir, path)

        return os.path.join(
            self.base_dir, step_output_handle.step_key, step_output_handle.output_name
        )

    def _get_new_id(self):
        return str(uuid.uuid4())

    def set_asset(self, _context, step_output_handle, obj, asset_metadata):
        """
        store data object to file and track it as AddressablAsset
        """
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)
        check.dict_param(asset_metadata, "asset_metadata", key_type=str)

        path = check.opt_str_param(asset_metadata.get("path"), "asset_metadata.path")
        filepath = self._get_path(step_output_handle, path)

        # Ensure path exists
        mkdir_p(os.path.dirname(filepath))

        with open(filepath, self.write_mode) as write_obj:
            pickle.dump(obj, write_obj, PICKLE_PROTOCOL)

        return PickledObjectFileystemAssetAddress(
            asset_id=self._get_new_id(), asset_metadata=asset_metadata
        )

    def get_asset(self, _context, step_output_handle, asset_metadata):
        """
        load data object from file using AssetAddress
        """
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)
        check.dict_param(asset_metadata, "asset_metadata", key_type=str)

        path = check.opt_str_param(asset_metadata.get("path"), "asset_metadata.path")
        filepath = self._get_path(step_output_handle, path)

        with open(filepath, self.read_mode) as read_obj:
            return pickle.load(read_obj)


@resource(config_schema={"base_dir": Field(StringSource, default_value=".", is_required=False)})
def default_filesystem_asset_store(init_context):
    return PickledObjectFileystemAssetStore(init_context.resource_config["base_dir"])
