import sys

from dagster import DagsterInstance, file_relative_path
from dagster.cli.workspace.workspace import Workspace
from dagster.core.host_representation.origin import ManagedGrpcPythonEnvRepositoryLocationOrigin
from dagster.core.host_representation.selector import PipelineSelector
from dagster.core.types.loadable_target_origin import LoadableTargetOrigin
from dagster.core.utils import make_new_run_id
from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION
from dagster_graphql.implementation.context import DagsterGraphQLContext
from dagster_graphql.test.utils import execute_dagster_graphql
from trigger_pipeline.trigger_pipeline.repo import my_repo

# start_trigger_marker_0

repo_location_origin = ManagedGrpcPythonEnvRepositoryLocationOrigin(
    LoadableTargetOrigin(
        executable_path=sys.executable, python_file=file_relative_path(__file__, "./repo.py")
    )
)

selector = PipelineSelector(
    location_name=repo_location_origin.location_name,
    repository_name=my_repo.name,
    pipeline_name="do_math",
    solid_selection=None,
).to_graphql_input()

workspace = Workspace([repo_location_origin])
run_config_data = {
    "solids": {"add_one": {"inputs": {"num": 5}}, "add_two": {"inputs": {"num": 6}},}
}
run_id = make_new_run_id()
mode = "default"
# end_trigger_marker_0

# start_trigger_marker_1
graphql_context = DagsterGraphQLContext(workspace=workspace, instance=DagsterInstance.get())
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
# end_trigger_marker_1
