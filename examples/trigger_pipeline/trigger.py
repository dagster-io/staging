import sys

from dagster import DagsterInstance
from dagster.cli.workspace.workspace import Workspace
from dagster.core.host_representation.origin import ManagedGrpcPythonEnvRepositoryLocationOrigin
from dagster.core.host_representation.selector import PipelineSelector
from dagster.core.types.loadable_target_origin import LoadableTargetOrigin
from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION
from dagster_graphql.implementation.context import DagsterGraphQLContext
from dagster_graphql.test.utils import execute_dagster_graphql
from repo import my_repo

repo_location_origin = ManagedGrpcPythonEnvRepositoryLocationOrigin(
    LoadableTargetOrigin(executable_path=sys.executable, python_file="./repo.py")
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


graphql_context = DagsterGraphQLContext(workspace=workspace, instance=DagsterInstance.get())
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
