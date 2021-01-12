from dagster_graphql.client.query import LAUNCH_PIPELINE_EXECUTION_MUTATION
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from helper import mode, pipeline_name, repository_name, run_config

variables = {
    "executionParams": {
        "selector": {
            "repositoryLocationName": "repo.py",
            "repositoryName": repository_name,
            "pipelineName": pipeline_name,
        },
        "runConfigData": run_config,
        "mode": mode,
    }
}

transport = RequestsHTTPTransport(url="http://localhost:3000/graphql", verify=True, retries=5,)

client = Client(transport=transport, fetch_schema_from_transport=True)

result = client.execute(gql(LAUNCH_PIPELINE_EXECUTION_MUTATION), variable_values=variables)
