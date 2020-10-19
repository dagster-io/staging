import os

from dagster import ModeDefinition, OutputDefinition, execute_pipeline, pipeline, seven, solid
from dagster.core.definitions.events import AddressableAssetOperationType
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.storage.asset_store import default_filesystem_asset_store


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
            addressable_asset_operation_events[0].event_specific_data.address.filepath
            == test_asset_metadata_dict["solid_a"]["path"]
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
            addressable_asset_operation_events[2].event_specific_data.address.filepath
            == test_asset_metadata_dict["solid_b"]["path"]
        )
