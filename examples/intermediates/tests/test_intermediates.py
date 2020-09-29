from dagster import DagsterInstance, execute_pipeline, reexecute_pipeline

from ..pickled_object import model_pipeline


def test_notif_all_pipeline():
    instance = DagsterInstance.ephemeral()

    result = execute_pipeline(model_pipeline, preset="local", instance=instance)
    assert result.success

    re1_result = reexecute_pipeline(
        model_pipeline,
        result.run_id,
        preset="local",
        instance=instance,
        step_selection=["parse_df.compute"],
    )
    assert re1_result.success

    re2_result = reexecute_pipeline(
        model_pipeline,
        re1_result.run_id,
        preset="local",
        instance=instance,
        step_selection=["parse_df.compute"],
    )
    assert re2_result.success
