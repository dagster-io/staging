import os

from dagster import Int, seven
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.instance import DagsterInstance
from dagster.core.storage.asset_store import PickledObjectFileystemAssetStore
from dagster.core.storage.intermediate_storage import build_fs_intermediate_storage


def test_address_path_operation_using_intermediates_file_system():
    with seven.TemporaryDirectory() as tmpdir_path:
        asset_store = PickledObjectFileystemAssetStore(tmpdir_path)
        output_address = "solid1.output"
        output_value = 5

        instance = DagsterInstance.ephemeral()
        intermediate_storage = build_fs_intermediate_storage(
            instance.intermediates_directory, run_id="some_run_id"
        )

        addressable_asset_operation = intermediate_storage.set_addressable_asset(
            context=None,
            asset_store=asset_store,
            step_output_handle=StepOutputHandle("solid1.compute"),
            value=output_value,
            path=output_address,
        )

        assert addressable_asset_operation.address.path == os.path.join(tmpdir_path, output_address)
        assert addressable_asset_operation.step_output_handle == StepOutputHandle("solid1.compute")

        assert (
            output_value
            == intermediate_storage.get_intermediate(
                context=None,
                dagster_type=Int,
                step_output_handle=StepOutputHandle("solid1.compute"),
            ).obj
        )
