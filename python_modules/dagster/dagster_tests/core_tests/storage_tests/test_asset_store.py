import os

from dagster import (
    DagsterInstance,
    ModeDefinition,
    OutputDefinition,
    execute_pipeline,
    pipeline,
    seven,
    solid,
)
from dagster.core.definitions.events import AddressableAssetOperationType
from dagster.core.execution.api import create_execution_plan, execute_plan
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.storage.address_storage import AddressStorage
from dagster.core.storage.asset_store import AssetStoreHandle, default_filesystem_asset_store


def define_asset_pipeline(asset_store, asset_metadata_dict):
    @solid(
        output_defs=[
            OutputDefinition(
                asset_store_key="default_fs_asset_store",
                asset_metadata=asset_metadata_dict["solid_a"],
            )
        ],
    )
    def solid_a(_context):
        return [1, 2, 3]

    @solid(
        output_defs=[
            OutputDefinition(
                asset_store_key="default_fs_asset_store",
                asset_metadata=asset_metadata_dict["solid_b"],
            )
        ],
    )
    def solid_b(_context, _df):
        return 1

    @pipeline(
        mode_defs=[ModeDefinition("local", resource_defs={"default_fs_asset_store": asset_store})]
    )
    def asset_pipeline():
        solid_b(solid_a())

    return asset_pipeline


def test_default_asset_store():
    with seven.TemporaryDirectory() as tmpdir_path:
        default_asset_store = default_filesystem_asset_store
        test_asset_metadata_dict = {
            "solid_a": {"path": os.path.join(tmpdir_path, "a")},
            "solid_b": {"path": os.path.join(tmpdir_path, "b")},
        }
        pipeline_def = define_asset_pipeline(default_asset_store, test_asset_metadata_dict)

        result = execute_pipeline(pipeline_def)
        assert result.success

        addressable_asset_operation_events = list(
            filter(lambda evt: evt.is_addressable_asset_operation, result.event_list)
        )

        assert len(addressable_asset_operation_events) == 3
        # SET ASSET for step "solid_a.compute" output "result"
        assert (
            addressable_asset_operation_events[0].event_specific_data.op
            == AddressableAssetOperationType.SET_ASSET
        )
        assert (
            addressable_asset_operation_events[0].event_specific_data.address.asset_metadata
            == test_asset_metadata_dict["solid_a"]
        )

        # GET ASSET for step "solid_b.compute" input "_df"
        assert (
            addressable_asset_operation_events[1].event_specific_data.op
            == AddressableAssetOperationType.GET_ASSET
        )
        assert (
            StepOutputHandle("solid_a.compute", "result")
            == addressable_asset_operation_events[1].event_specific_data.step_output_handle
        )

        # SET ASSET for step "solid_b.compute" output "result"
        assert (
            addressable_asset_operation_events[2].event_specific_data.op
            == AddressableAssetOperationType.SET_ASSET
        )
        assert (
            addressable_asset_operation_events[2].event_specific_data.address.asset_metadata
            == test_asset_metadata_dict["solid_b"]
        )


def execute_pipeline_with_steps(pipeline_def, step_keys_to_execute=None):
    plan = create_execution_plan(pipeline_def, step_keys_to_execute=step_keys_to_execute)
    with DagsterInstance.ephemeral() as instance:
        pipeline_run = instance.create_run_for_pipeline(
            pipeline_def=pipeline_def, step_keys_to_execute=step_keys_to_execute,
        )
        return execute_plan(plan, instance, pipeline_run)


def test_step_subset():
    with seven.TemporaryDirectory() as tmpdir_path:
        default_asset_store = default_filesystem_asset_store
        test_asset_metadata_dict = {
            "solid_a": {"path": os.path.join(tmpdir_path, "a")},
            "solid_b": {"path": os.path.join(tmpdir_path, "b")},
        }

        pipeline_def = define_asset_pipeline(default_asset_store, test_asset_metadata_dict)
        events = execute_pipeline_with_steps(pipeline_def)
        for evt in events:
            assert not evt.is_failure

        step_subset_events = execute_pipeline_with_steps(
            pipeline_def, step_keys_to_execute=["solid_b.compute"]
        )
        for evt in step_subset_events:
            assert not evt.is_failure
        # only the selected step subset was executed
        assert set([evt.step_key for evt in step_subset_events]) == {"solid_b.compute"}


def test_address_storage():
    with seven.TemporaryDirectory() as tmpdir_path:
        test_asset_store = default_filesystem_asset_store
        # .configured({"base_dir": tmpdir_path})
        test_asset_metadata_dict = {
            "solid_a": {"path": os.path.join(tmpdir_path, "a")},
            "solid_b": {"path": os.path.join(tmpdir_path, "b")},
        }
        pipeline_def = define_asset_pipeline(test_asset_store, test_asset_metadata_dict,)

        instance = DagsterInstance.ephemeral(address_storage=AddressStorage())
        result = execute_pipeline(pipeline_def, instance=instance)
        assert result.success

        assert instance.address_storage
        asset_address_a, asset_store_handle_a = instance.address_storage.mapping[
            StepOutputHandle("solid_a.compute", "result")
        ]
        assert asset_address_a.asset_metadata == test_asset_metadata_dict["solid_a"]
        assert asset_store_handle_a == AssetStoreHandle(
            asset_store_key="default_fs_asset_store",
            asset_metadata=test_asset_metadata_dict["solid_a"],
        )

        asset_address_b, asset_store_handle_b = instance.address_storage.mapping[
            StepOutputHandle("solid_b.compute", "result")
        ]
        assert asset_address_b.asset_metadata == test_asset_metadata_dict["solid_b"]
        assert asset_store_handle_b == AssetStoreHandle(
            asset_store_key="default_fs_asset_store",
            asset_metadata=test_asset_metadata_dict["solid_b"],
        )
