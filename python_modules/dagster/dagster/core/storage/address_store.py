from dagster import check
from dagster.core.definitions.events import AddressableAssetOperation, AddressableAssetOperationType
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.storage.asset_store import AssetAddress, AssetStore, AssetStoreHandle


class AddressStore:
    def __init__(self, mapping=None):
        self.mapping = check.opt_dict_param(mapping, "mapping", key_type=StepOutputHandle)

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
