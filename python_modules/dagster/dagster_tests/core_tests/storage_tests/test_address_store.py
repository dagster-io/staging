import os

from dagster import ModeDefinition, OutputDefinition, execute_pipeline, pipeline, seven, solid
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.instance import DagsterInstance
from dagster.core.storage.address_store import AddressStore
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
        pass

    @pipeline(
        mode_defs=[ModeDefinition("local", resource_defs={"default_fs_asset_store": asset_store},)]
    )
    def asset_pipeline():
        solid_b(solid_a())

    return asset_pipeline


def test_address_store():
    with seven.TemporaryDirectory() as tmpdir_path:
        test_asset_store = default_filesystem_asset_store
        # .configured({"base_dir": tmpdir_path})
        test_asset_metadata_dict = {
            "solid_a": {"path": os.path.join(tmpdir_path, "a")},
            "solid_b": {"path": os.path.join(tmpdir_path, "b")},
        }
        pipeline_def = define_asset_pipeline(test_asset_store, test_asset_metadata_dict,)

        instance = DagsterInstance.ephemeral(address_store=AddressStore())
        result = execute_pipeline(pipeline_def, instance=instance)
        assert result.success

        assert instance.address_store
        asset_address_a, asset_store_handle_a = instance.address_store.mapping[
            StepOutputHandle("solid_a.compute", "result")
        ]
        assert asset_address_a.filepath == test_asset_metadata_dict["solid_a"]["path"]
        assert asset_store_handle_a == AssetStoreHandle(
            asset_store_key="default_fs_asset_store",
            asset_metadata=test_asset_metadata_dict["solid_a"],
        )

        asset_address_b, asset_store_handle_b = instance.address_store.mapping[
            StepOutputHandle("solid_b.compute", "result")
        ]
        assert asset_address_b.filepath == test_asset_metadata_dict["solid_b"]["path"]
        assert asset_store_handle_b == AssetStoreHandle(
            asset_store_key="default_fs_asset_store",
            asset_metadata=test_asset_metadata_dict["solid_b"],
        )
