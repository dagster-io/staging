import os
import pickle

from dagster import DagsterInstance, execute_pipeline, reexecute_pipeline, seven
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.storage.address_storage import DurableAddressStorage

from ..pickled_object import model_pipeline


def test_asset_store():
    with seven.TemporaryDirectory() as tmpdir_path:

        run_config = {
            "resources": {"default_fs_asset_store": {"config": {"base_dir": tmpdir_path}}},
        }

        result = execute_pipeline(model_pipeline, run_config=run_config, mode="test")

        assert result.success

        filepath_call_api = os.path.join(tmpdir_path, result.run_id, "call_api.compute", "result")
        assert os.path.isfile(filepath_call_api)
        with open(filepath_call_api, "rb") as read_obj:
            assert pickle.load(read_obj) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        filepath_parse_df = os.path.join(tmpdir_path, result.run_id, "parse_df.compute", "result")
        assert os.path.isfile(filepath_parse_df)
        with open(filepath_parse_df, "rb") as read_obj:
            assert pickle.load(read_obj) == [1, 2, 3, 4, 5]


def test_address_storage():
    with seven.TemporaryDirectory() as tmpdir_path:

        run_config = {
            "resources": {"default_fs_asset_store": {"config": {"base_dir": tmpdir_path}}},
        }

        result = execute_pipeline(model_pipeline, run_config=run_config, mode="test")

        assert result.success

        filepath_call_api = os.path.join(tmpdir_path, result.run_id, "call_api.compute", "result")
        assert os.path.isfile(filepath_call_api)
        with open(filepath_call_api, "rb") as read_obj:
            assert pickle.load(read_obj) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        filepath_parse_df = os.path.join(tmpdir_path, result.run_id, "parse_df.compute", "result")
        assert os.path.isfile(filepath_parse_df)
        with open(filepath_parse_df, "rb") as read_obj:
            assert pickle.load(read_obj) == [1, 2, 3, 4, 5]
        instance = DagsterInstance.ephemeral(address_storage=DurableAddressStorage())
        run_config = {
            "resources": {"default_fs_asset_store": {"config": {"base_dir": tmpdir_path}}},
            "storage": {"filesystem": {}},
        }

        result = execute_pipeline(
            model_pipeline, run_config=run_config, mode="test", instance=instance
        )

        assert result.success
        assert len(instance.address_storage.mapping.keys()) == 3
        _step_output_handle = StepOutputHandle("call_api.compute", "result")
        _address = instance.address_storage.mapping[_step_output_handle][0]

        re1_result = reexecute_pipeline(
            model_pipeline,
            result.run_id,
            run_config=run_config,
            mode="test",
            instance=instance,
            step_selection=["parse_df.compute"],
        )
        assert re1_result.success
        assert instance.address_storage.mapping[_step_output_handle][0] == _address

        re2_result = reexecute_pipeline(
            model_pipeline,
            re1_result.run_id,
            run_config=run_config,
            mode="test",
            instance=instance,
            step_selection=["parse_df.compute"],
        )
        assert re2_result.success
        assert instance.address_storage.mapping[_step_output_handle][0] == _address
