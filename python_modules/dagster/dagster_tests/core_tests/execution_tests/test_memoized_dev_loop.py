import tempfile

from dagster import execute_pipeline, reexecute_pipeline
from dagster.core.execution.api import create_execution_plan
from dagster.core.test_utils import instance_for_test

from .memoized_dev_loop_pipeline import asset_pipeline


def get_step_keys_to_execute(pipeline, run_config, mode, instance):
    return create_execution_plan(pipeline, run_config, mode, instance=instance).step_keys_to_execute


def test_dev_loop_changing_versions():
    with tempfile.TemporaryDirectory() as temp_dir:
        with instance_for_test(temp_dir=temp_dir) as instance:

            run_config = {
                "solids": {
                    "create_string_1_asset": {"config": {"input_str": "apple"}},
                    "take_string_1_asset": {"config": {"input_str": "apple"}},
                },
                "resources": {"io_manager": {"config": {"base_dir": temp_dir}}},
            }

            result = execute_pipeline(
                asset_pipeline,
                run_config=run_config,
                mode="only_mode",
                tags={"dagster/is_memoized_run": "true"},
                instance=instance,
            )
            assert result.success
            assert not get_step_keys_to_execute(asset_pipeline, run_config, "only_mode", instance)

            run_config["solids"]["take_string_1_asset"]["config"]["input_str"] = "banana"

            assert get_step_keys_to_execute(asset_pipeline, run_config, "only_mode", instance) == [
                "take_string_1_asset"
            ]
            result = reexecute_pipeline(
                asset_pipeline,
                parent_run_id=result.run_id,
                run_config=run_config,
                mode="only_mode",
                tags={"dagster/is_memoized_run": "true"},
                instance=instance,
            )
            assert result.success
            assert not get_step_keys_to_execute(asset_pipeline, run_config, "only_mode", instance)

            result = execute_pipeline(
                asset_pipeline,
                run_config=run_config,
                mode="only_mode",
                tags={"dagster/is_memoized_run": "true"},
                instance=instance,
            )
            assert result.success
            assert len(result.step_event_list) == 0
