import json

import pytest
from dagit import app
from dagster import DagsterInstance, execute_pipeline, file_relative_path
from dagster.cli.workspace import get_workspace_from_kwargs
from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION
from dagster_graphql.implementation.context import DagsterGraphQLContext
from dagster_graphql.test.utils import infer_pipeline_selector
from trigger_pipeline.trigger_pipeline.helper import (
    mode,
    pipeline_name,
    repository_name,
    run_config,
)
from trigger_pipeline.trigger_pipeline.repo import do_math


def test_trigger_do_math_pipeline():
    res = execute_pipeline(do_math, run_config=run_config)
    assert res.success
    assert res.result_for_solid("subtract").output_value() == -2


@pytest.mark.parametrize("get_instance", [DagsterInstance.ephemeral])
def test_trigger_pipeline_by_gql(get_instance):
    assert repository_name == "my_repo"
    assert pipeline_name == "do_math"
    assert mode == "default"

    with get_instance() as instance:
        with get_workspace_from_kwargs(
            {
                "python_file": file_relative_path(__file__, "../trigger_pipeline/repo.py"),
                "attribute": "do_math",
            }
        ) as workspace:

            flask_app = app.create_app_from_workspace(workspace, instance)
            app_client = flask_app.test_client()

            graphql_context = DagsterGraphQLContext(instance, workspace)
            selector = infer_pipeline_selector(graphql_context, pipeline_name)
            variables = {
                "executionParams": {
                    "selector": selector,
                    "runConfigData": run_config,
                    "mode": mode,
                }
            }

            result = app_client.post(
                "/graphql?query={query_string}&variables={variables}".format(
                    query_string=LAUNCH_PIPELINE_EXECUTION_MUTATION,
                    variables=json.dumps(variables),
                )
            )
            data = json.loads(result.data.decode("utf-8"))
            assert (
                data["data"]["launchPipelineExecution"]["__typename"] == "LaunchPipelineRunSuccess"
            )
