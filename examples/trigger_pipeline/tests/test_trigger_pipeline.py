from dagster import execute_pipeline
from dagster.core.utils import make_new_run_id
from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION
from dagster_graphql.test.utils import execute_dagster_graphql
from trigger_pipeline.trigger_pipeline.repo import do_math
from trigger_pipeline.trigger_pipeline.trigger import (
    graphql_context,
    mode,
    repo_location_origin,
    run_config_data,
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
    run_id = make_new_run_id()
    result = execute_dagster_graphql(
        graphql_context,
        LAUNCH_PIPELINE_EXECUTION_MUTATION,
        variables={
            "executionParams": {
                "selector": selector,
                "runConfigData": run_config_data,
                "mode": mode,
                "executionMetadata": {"runId": run_id},
            }
        },
    )
    assert selector["pipelineName"] == "do_math"
    assert mode == "default"
    assert result.data["launchPipelineExecution"]["__typename"] == "LaunchPipelineRunSuccess"
    assert result.data["launchPipelineExecution"]["run"]["runId"] == run_id
