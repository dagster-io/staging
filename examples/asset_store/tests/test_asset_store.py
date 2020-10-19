from dagster import execute_pipeline, seven

from ..pickled_object import model_pipeline


def test_pickled_object():
    with seven.TemporaryDirectory() as tmpdir_path:

        run_config = {
            "resources": {"default_fs_asset_store": {"config": {"base_dir": tmpdir_path}}},
        }

        result = execute_pipeline(model_pipeline, run_config=run_config, mode="test")

        assert result.success
