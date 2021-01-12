from dagster import check, execute_pipeline
from dagster.core.instance import DagsterInstance
from trigger_pipeline.trigger_pipeline.repo import do_math
from trigger_pipeline.trigger_pipeline.trigger import (
    dagster_instance,
    mode,
    repo_location_origin,
    result,
    run_config_data,
    run_id,
    selector,
    workspace,
)


def test_trigger_do_math_pipeline():
    res = execute_pipeline(do_math, run_config=run_config_data)
    assert res.success
    assert res.result_for_solid("subtract").output_value() == -2


def test_workspace_exist():
    assert repo_location_origin.location_name == "repo.py"
    assert len(workspace.repository_location_names) == 1


def test_trigger_pipeline_by_gql():
    check.inst_param(dagster_instance, "instance", DagsterInstance)
    assert result.data["launchPipelineExecution"]["__typename"] == "LaunchPipelineRunSuccess"
    assert selector["pipelineName"] == "do_math"
    assert mode == "default"
    assert result.data["launchPipelineExecution"]["run"]["runId"] == run_id
