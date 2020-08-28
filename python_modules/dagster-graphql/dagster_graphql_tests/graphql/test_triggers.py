from dagster_graphql.client.query import LAUNCH_TRIGGERED_EXECUTION
from dagster_graphql.test.utils import execute_dagster_graphql, infer_repository_selector

from .graphql_context_test_suite import (
    OutOfProcessExecutingGraphQLContextTestMatrix,
    ReadonlyGraphQLContextTestMatrix,
)

GET_TRIGGERED_EXECUTIONS_QUERY = """
    query TriggeredExecutionsQuery($repositorySelector: RepositorySelector!) {
        triggerDefinitionsOrError(repositorySelector: $repositorySelector) {
            __typename
            ... on TriggerDefinitions {
                results {
                    name
                    pipelineName
                    solidSelection
                    mode
                }
            }
            ... on PythonError {
                message
                stack
            }
        }
    }
"""

GET_TRIGGERED_EXECUTION_QUERY = """
    query TriggeredExecutionQuery($repositorySelector: RepositorySelector!, $triggerName: String!) {
        triggerDefinitionOrError(repositorySelector: $repositorySelector, triggerName: $triggerName) {
            __typename
            ... on TriggerDefinition {
                name
                pipelineName
                solidSelection
                mode
                tagsOrError {
                    ... on TagsList {
                        results {
                            key
                            value
                        }
                    }
                }
                runConfigOrError {
                    ... on RunConfig {
                        yaml
                    }
                }
            }
            ... on PythonError {
                message
                stack
            }
        }
    }
"""


class TestTriggeredExecutions(ReadonlyGraphQLContextTestMatrix):
    def test_get_triggered_executions(self, graphql_context, snapshot):
        selector = infer_repository_selector(graphql_context)
        result = execute_dagster_graphql(
            graphql_context,
            GET_TRIGGERED_EXECUTIONS_QUERY,
            variables={"repositorySelector": selector},
        )

        assert result.data
        snapshot.assert_match(result.data)

    def test_get_triggered_execution(self, graphql_context, snapshot):
        selector = infer_repository_selector(graphql_context)
        result = execute_dagster_graphql(
            graphql_context,
            GET_TRIGGERED_EXECUTION_QUERY,
            variables={"triggerName": "triggered_no_config", "repositorySelector": selector},
        )

        assert result.data
        snapshot.assert_match(result.data)

        invalid_triggered_execution_result = execute_dagster_graphql(
            graphql_context,
            GET_TRIGGERED_EXECUTION_QUERY,
            variables={"triggerName": "bogus_trigger", "repositorySelector": selector},
        )

        assert (
            invalid_triggered_execution_result.data["triggerDefinitionOrError"]["__typename"]
            == "TriggerDefinitionNotFoundError"
        )
        assert invalid_triggered_execution_result.data

        snapshot.assert_match(invalid_triggered_execution_result.data)


class TestTriggerMutation(OutOfProcessExecutingGraphQLContextTestMatrix):
    def test_get_partition_runs(self, graphql_context):
        repository_selector = infer_repository_selector(graphql_context)
        result = execute_dagster_graphql(
            graphql_context,
            LAUNCH_TRIGGERED_EXECUTION,
            variables={
                "triggerSelector": {
                    "repositoryName": repository_selector["repositoryName"],
                    "repositoryLocationName": repository_selector["repositoryLocationName"],
                    "triggerName": "triggered_no_config",
                }
            },
        )
        assert not result.errors
        assert result.data["triggerExecution"]["__typename"] == "TriggerExecutionSuccess"
        assert len(result.data["triggerExecution"]["launchedRunIds"]) == 1
