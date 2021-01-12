from dagster import execute_pipeline
from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION
from dagster_graphql.test.utils import execute_dagster_graphql

from ..execute_pipeline_graphql.repo import do_math
from ..execute_pipeline_graphql.trigger import graphql_context, run_config_data, selector


def test_trigger_do_math_pipeline():
    res = execute_pipeline(
        do_math,
        run_config={
            "solids": {"add_one": {"inputs": {"num": 5}}, "add_two": {"inputs": {"num": 6}},}
        },
    )
    assert res.success


def test_trigger_pipeline_by_gql():
    result = execute_dagster_graphql(
        graphql_context,
        LAUNCH_PIPELINE_EXECUTION_MUTATION,
        variables={
            "executionParams": {
                "selector": selector,
                "runConfigData": run_config_data,
                "mode": "default",
            }
        },
    )
    assert result.data["launchPipelineExecution"]["__typename"] == "LaunchPipelineRunSuccess"
