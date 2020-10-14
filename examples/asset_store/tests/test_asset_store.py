from dagster import DagsterInstance, execute_pipeline, reexecute_pipeline, seven
from dagster.core.execution.plan.objects import StepOutputHandle
from dagster.core.storage.address_storage import DurableAddressStorage

from ..pickled_object import model_pipeline


def test_pickled_object():
    with seven.TemporaryDirectory() as tmpdir_path:

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
