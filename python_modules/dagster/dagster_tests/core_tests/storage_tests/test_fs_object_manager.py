import os
import pickle

from dagster import ModeDefinition, execute_pipeline, pipeline, seven, solid
from dagster.core.storage.fs_object_manager import fs_object_manager


def define_pipeline(object_manager):
    @solid
    def solid_a(_context):
        return [1, 2, 3]

    @solid
    def solid_b(_context, _df):
        return 1

    @pipeline(mode_defs=[ModeDefinition("local", resource_defs={"object_manager": object_manager})])
    def asset_pipeline():
        solid_b(solid_a())

    return asset_pipeline


def test_fs_object_manager():
    with seven.TemporaryDirectory() as tmpdir_path:
        asset_store = fs_object_manager.configured({"base_dir": tmpdir_path})
        pipeline_def = define_pipeline(asset_store)

        result = execute_pipeline(pipeline_def)
        assert result.success

        handled_output_events = list(filter(lambda evt: evt.is_handled_output, result.event_list))

        assert len(handled_output_events) == 2
        filepath_a = os.path.join(tmpdir_path, result.run_id, "solid_a.compute", "result")
        assert os.path.isfile(filepath_a)
        with open(filepath_a, "rb") as read_obj:
            assert pickle.load(read_obj) == [1, 2, 3]

        loaded_input_events = list(filter(lambda evt: evt.is_loaded_input, result.event_list))
        assert len(loaded_input_events) == 1
        assert "solid_a.compute" == loaded_input_events[0].event_specific_data.upstream_step_key

        filepath_b = os.path.join(tmpdir_path, result.run_id, "solid_b.compute", "result")
        assert os.path.isfile(filepath_b)
        with open(filepath_b, "rb") as read_obj:
            assert pickle.load(read_obj) == 1
