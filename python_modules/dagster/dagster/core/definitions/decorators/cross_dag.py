# This doesn't belong in "decorators", need a place to put it.
from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION
from dagster_graphql.implementation.context import DagsterGraphQLContext
from dagster_graphql.test.utils import execute_dagster_graphql

from dagster import DagsterInstance, check, resource
from dagster.cli.workspace.workspace import Workspace


@resource
def launch_pipeline_run_resource(_):
    def launch_pipeline_run(
        instance,
        mode,
        run_id,
        should_execute_pipeline_fn,
        selector_fn,
        run_config_data_fn,
        workspace_fn,
    ):
        check.inst_param(instance, 'instance', DagsterInstance)
        check.str_param(mode, 'mode')
        check.str_param(run_id, 'run_id')
        check.callable_param(should_execute_pipeline_fn, 'should_execute_pipeline_fn')
        check.callable_param(selector_fn, 'selector_fn')
        check.callable_param(run_config_data_fn, 'run_config_data_fn')
        check.callable_param(workspace_fn, 'workspace_fn')

        should_execute = should_execute_pipeline_fn()

        if not should_execute:
            return False

        workspace = workspace_fn()
        check.inst_param(workspace, 'workspace', Workspace)

        selector = selector_fn()
        check.opt_dict_param(selector, 'selector')

        run_config_data = run_config_data_fn()
        check.opt_dict_param(run_config_data, 'run_config_data')

        graphql_context = DagsterGraphQLContext(workspace=workspace, instance=instance)
        result = execute_dagster_graphql(
            graphql_context,
            LAUNCH_PIPELINE_EXECUTION_MUTATION,
            variables={
                "executionParams": {
                    "selector": selector,
                    "runConfigData": run_config_data,
                    "mode": mode,
                    "executionMetadata": {
                        "tags": [
                            {"key": 'cross_pipeline', "value": "true"},
                            {"key": 'cross_pipeline_run_id', "value": run_id},
                        ],
                    },
                }
            },
        )
        return result

    return launch_pipeline_run
