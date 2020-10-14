from dagster import check
from dagster.core.definitions.events import AddressableAssetOperation, AddressableAssetOperationType
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.storage.asset_store import AssetAddress, AssetStore, AssetStoreHandle
from dagster.serdes import ConfigurableClass


class AddressStorage(ConfigurableClass):
    def __init__(self, inst_data=None, mapping=None):
        self._inst_data = inst_data
        # mapping step output to an address (1:1 currently).
        # Note: with versioning, the mapping will potentially be 1:n where we will be able to track
        # multiple addresses with the same step output at the instance level.
        self.mapping = check.opt_dict_param(mapping, "mapping", key_type=StepOutputHandle)

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @staticmethod
    def from_config_value(inst_data, config_value):
        return AddressStorage(inst_data=inst_data)

    def _get_asset_store(self, context, asset_store_handle):
        asset_store = getattr(context.resources, asset_store_handle.asset_store_key)
        return check.inst(asset_store, AssetStore)

    def get_addressable_asset(self, context, step_output_handle):
        address, asset_store_handle = self.mapping.get(step_output_handle, None)
        check.inst(address, AssetAddress)
        check.inst(asset_store_handle, AssetStoreHandle)

        asset_store = self._get_asset_store(context, asset_store_handle)

        obj = asset_store.get_asset(context, address)
        return AddressableAssetOperation(
            AddressableAssetOperationType.GET_ASSET,
            address,
            step_output_handle,
            asset_store_handle,
            obj=obj,
        )

    def set_addressable_asset(self, context, asset_store_handle, step_output_handle, value):
        check.inst_param(asset_store_handle, "asset_store_handle", AssetStoreHandle)
        asset_store = self._get_asset_store(context, asset_store_handle)
        address = asset_store.set_asset(context, value, asset_store_handle.asset_metadata)
        check.inst(address, AssetAddress)

        self.mapping[step_output_handle] = (address, asset_store_handle)
        return AddressableAssetOperation(
            AddressableAssetOperationType.SET_ASSET, address, step_output_handle, asset_store_handle
        )

    def has_addressable_asset(self, step_output_handle):
        check.inst_param(step_output_handle, "step_output_handle", StepOutputHandle)
        return self.mapping.get(step_output_handle) is not None
