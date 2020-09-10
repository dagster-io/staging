# This doesn't belong in "decorators", need a place to put it.
from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION
from dagster_graphql.implementation.context import DagsterGraphQLContext
from dagster_graphql.test.utils import execute_dagster_graphql

from dagster import Field, Shape, String, check, configured, resource
from dagster.cli.workspace.workspace import Workspace
from dagster.core.execution.context.step import StepExecutionContext
from dagster.utils.merger import deep_merge_dicts


def define_selector_config():
    fields = {
        'repositoryLocationName': Field(String),
        'repositoryName': Field(String),
        'pipelineName': Field(String),
        'solidSelection': Field(list),
    }
    return Field(Shape(fields=fields), description="Selector config")


def launch_pipeline_run_resource_factory(selector_dict):
    @resource(config_schema={'selector': define_selector_config()})
    def launch_pipeline_run_resource(init_context):
        def launch_pipeline_run(
            solid_context, should_execute_pipeline_fn, execution_params_fn, workspace_fn,
        ):
            check.inst_param(solid_context, 'solid_context', StepExecutionContext)
            check.callable_param(should_execute_pipeline_fn, 'should_execute_pipeline_fn')
            check.callable_param(execution_params_fn, 'execution_params_fn')
            check.callable_param(workspace_fn, 'workspace_fn')

            should_execute = should_execute_pipeline_fn(solid_context)

            if not should_execute:
                return False

            instance = solid_context.instance
            mode = solid_context.pipeline_run.mode
            run_id = solid_context.pipeline_run.run_id

            workspace = workspace_fn()
            check.inst_param(workspace, 'workspace', Workspace)

            selector_dict = init_context.resource_config["selector"]
            # solid_context.log.info(selector_dict)
            execution_params = execution_params_fn()
            execution_params["executionMetadata"] = deep_merge_dicts(
                execution_params.get("executionMetadata", {}),
                {"tags": [{"key": 'launched_from_run_id', "value": run_id}]},
            )

            execution_params["selector"] = selector_dict

            # Allow user to override since "mode" can have different meanings across repo
            if not execution_params.get("mode"):
                execution_params["mode"] = mode

            graphql_context = DagsterGraphQLContext(workspace=workspace, instance=instance)
            result = execute_dagster_graphql(
                graphql_context,
                LAUNCH_PIPELINE_EXECUTION_MUTATION,
                variables={"executionParams": execution_params},
            )
            return result

        return launch_pipeline_run

    return configured(launch_pipeline_run_resource)({"selector": selector_dict})
