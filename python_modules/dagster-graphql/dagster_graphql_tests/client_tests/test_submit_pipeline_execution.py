import pytest
from dagster_graphql.client import DagsterGraphQLClientError

from .conftest import MockClient

EXPECTED_RUN_ID = "foo"


def test_success(mock_client: MockClient):
    response = {
        "launchPipelineExecution": {
            "__typename": "LaunchPipelineRunSuccess",
            "run": {"runId": EXPECTED_RUN_ID},
        }
    }
    mock_client.mock_gql_client.execute.return_value = response
    actual_run_id = mock_client.python_client.submit_pipeline_execution(
        "bar",
        repository_location_name="baz",
        repository_name="quux",
        run_config_data={},
        mode="default",
    )
    assert actual_run_id == EXPECTED_RUN_ID


def test_preset_success(mock_client: MockClient):
    response = {
        "launchPipelineExecution": {
            "__typename": "LaunchPipelineRunSuccess",
            "run": {"runId": EXPECTED_RUN_ID},
        }
    }
    mock_client.mock_gql_client.execute.return_value = response
    actual_run_id = mock_client.python_client.submit_pipeline_execution(
        "bar", repository_location_name="baz", repository_name="quux", preset_name="cool_preset"
    )
    assert actual_run_id == EXPECTED_RUN_ID


def test_no_location_or_repo_provided_success(mock_client: MockClient):
    repo_loc_name, repo_name, pipeline_name = "bar", "baz", "quux"
    other_repo_name, other_pipeline_name = "other repo", "my_pipeline"
    get_locations_and_names_response = {
        "repositoriesOrError": {
            "__typename": "RepositoryConnection",
            "nodes": [
                {
                    "name": repo_name,
                    "location": {"name": repo_loc_name},
                    "pipelines": [{"name": pipeline_name}, {"name": other_pipeline_name}],
                },
                {
                    "name": other_repo_name,
                    "location": {"name": repo_loc_name},
                    "pipelines": [{"name": "fun pipeline"}, {"name": other_pipeline_name}],
                },
            ],
        }
    }
    submit_execution_response = {
        "launchPipelineExecution": {
            "__typename": "LaunchPipelineRunSuccess",
            "run": {"runId": EXPECTED_RUN_ID},
        }
    }
    mock_client.mock_gql_client.execute.side_effect = [
        get_locations_and_names_response,
        submit_execution_response,
    ]

    actual_run_id = mock_client.python_client.submit_pipeline_execution(
        pipeline_name, run_config_data={}, mode="default"
    )
    assert actual_run_id == EXPECTED_RUN_ID


def test_no_location_or_repo_provided_duplicate_pipeline_failure(mock_client: MockClient):
    repo_loc_name, repo_name, pipeline_name = "bar", "baz", "quux"
    other_repo_name = "other repo"
    get_locations_and_names_response = {
        "repositoriesOrError": {
            "__typename": "RepositoryConnection",
            "nodes": [
                {
                    "name": repo_name,
                    "location": {"name": repo_loc_name},
                    "pipelines": [{"name": pipeline_name}],
                },
                {
                    "name": other_repo_name,
                    "location": {"name": repo_loc_name},
                    "pipelines": [{"name": pipeline_name}],
                },
            ],
        }
    }
    submit_execution_response = {
        "launchPipelineExecution": {
            "__typename": "LaunchPipelineRunSuccess",
            "run": {"runId": EXPECTED_RUN_ID},
        }
    }
    mock_client.mock_gql_client.execute.side_effect = [
        get_locations_and_names_response,
        submit_execution_response,
    ]

    with pytest.raises(DagsterGraphQLClientError) as exc_info:
        mock_client.python_client.submit_pipeline_execution(
            pipeline_name, run_config_data={}, mode="default"
        )

    assert exc_info.value.args[0].find(f"multiple pipelines with the name {pipeline_name}") != -1


def test_no_location_or_repo_provided_no_pipeline_failure(mock_client: MockClient):
    repo_loc_name, repo_name, pipeline_name = "bar", "baz", "quux"
    get_locations_and_names_response = {
        "repositoriesOrError": {
            "__typename": "RepositoryConnection",
            "nodes": [
                {
                    "name": repo_name,
                    "location": {"name": repo_loc_name},
                    "pipelines": [{"name": pipeline_name}],
                }
            ],
        }
    }
    submit_execution_response = {
        "launchPipelineExecution": {
            "__typename": "LaunchPipelineRunSuccess",
            "run": {"runId": EXPECTED_RUN_ID},
        }
    }
    mock_client.mock_gql_client.execute.side_effect = [
        get_locations_and_names_response,
        submit_execution_response,
    ]

    with pytest.raises(DagsterGraphQLClientError) as exc_info:
        mock_client.python_client.submit_pipeline_execution(
            "123", run_config_data={}, mode="default"
        )

    assert exc_info.value.args[0] == "PipelineNotFoundError"


def test_failure_with_invalid_step_error(
    mock_client: MockClient,  # pylint: disable=unused-argument
):
    pass


def test_failure_with_invalid_output_error(
    mock_client: MockClient,  # pylint: disable=unused-argument
):
    pass


def test_failure_with_pipeline_config_invalid(
    mock_client: MockClient,  # pylint: disable=unused-argument
):
    pass


def test_failure_with_python_error(mock_client: MockClient):  # pylint: disable=unused-argument
    pass


def test_failure_with_query_error(mock_client: MockClient):  # pylint: disable=unused-argument
    pass
