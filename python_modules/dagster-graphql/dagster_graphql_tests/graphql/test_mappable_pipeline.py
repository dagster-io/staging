from dagster_graphql.client.query import (
    LAUNCH_PIPELINE_EXECUTION_MUTATION,
    LAUNCH_PIPELINE_REEXECUTION_MUTATION,
)
from dagster_graphql.test.utils import (
    execute_dagster_graphql_and_finish_runs,
    infer_pipeline_selector,
)

from dagster.core.storage.tags import RESUME_RETRY_TAG

from .utils import (
    get_all_logs_for_finished_run_via_subscription,
    step_did_fail,
    step_did_not_run,
    step_did_succeed,
)


def test_start_pipeline_execution(graphql_context):
    selector = infer_pipeline_selector(graphql_context, "mappable_pipeline")
    result = execute_dagster_graphql_and_finish_runs(
        graphql_context,
        LAUNCH_PIPELINE_EXECUTION_MUTATION,
        variables={
            "executionParams": {
                "selector": selector,
                "runConfigData": {
                    "solids": {"multiply_inputs": {"inputs": {"should_fail": {"value": True}}}},
                    "storage": {"filesystem": {}},
                },
                "mode": "default",
            }
        },
    )

    assert not result.errors
    assert result.data
    assert result.data["launchPipelineExecution"]["__typename"] == "LaunchPipelineRunSuccess"
    assert result.data["launchPipelineExecution"]["run"]["pipeline"]["name"] == "mappable_pipeline"

    parent_run_id = result.data["launchPipelineExecution"]["run"]["runId"]

    logs = get_all_logs_for_finished_run_via_subscription(graphql_context, parent_run_id)[
        "pipelineRunLogs"
    ]["messages"]

    assert step_did_succeed(logs, "emit.compute")
    assert step_did_succeed(logs, "multiply_inputs[0].compute")
    assert step_did_succeed(logs, "multiply_inputs[1].compute")
    assert step_did_fail(logs, "multiply_inputs[2].compute")
    assert step_did_succeed(logs, "multiply_by_two[0].compute")
    assert step_did_succeed(logs, "multiply_by_two[1].compute")

    retry_one = execute_dagster_graphql_and_finish_runs(
        graphql_context,
        LAUNCH_PIPELINE_REEXECUTION_MUTATION,
        variables={
            "executionParams": {
                "mode": "default",
                "selector": selector,
                "runConfigData": {
                    "solids": {"multiply_inputs": {"inputs": {"should_fail": {"value": True}}}},
                    "storage": {"filesystem": {}},
                    # "execution": {"multiprocess": {}},
                },
                "executionMetadata": {
                    "rootRunId": parent_run_id,
                    "parentRunId": parent_run_id,
                    "tags": [{"key": RESUME_RETRY_TAG, "value": "true"}],
                },
            }
        },
    )
    run_id = retry_one.data["launchPipelineReexecution"]["run"]["runId"]

    logs = get_all_logs_for_finished_run_via_subscription(graphql_context, run_id)[
        "pipelineRunLogs"
    ]["messages"]

    assert step_did_not_run(logs, "emit.compute")
    assert step_did_not_run(logs, "multiply_inputs[0].compute")
    assert step_did_not_run(logs, "multiply_inputs[1].compute")
    assert step_did_succeed(logs, "multiply_inputs[2].compute")

    assert step_did_not_run(logs, "multiply_by_two[0].compute")
    assert step_did_not_run(logs, "multiply_by_two[1].compute")
    assert step_did_succeed(logs, "multiply_by_two[2].compute")
