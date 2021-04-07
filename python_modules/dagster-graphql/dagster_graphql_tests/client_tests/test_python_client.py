from collections import namedtuple
from unittest.mock import patch

import pytest
from dagster_graphql.client import (
    DagsterGraphQLClient,
    DagsterGraphQLClientError,
    ReloadRepositoryLocationStatus,
)

MockClient = namedtuple("MockClient", ("python_client", "mock_gql_client"))


@pytest.fixture(scope="function", name="mock_client")
def create_mock_client():
    with patch("dagster_graphql.client.client.Client") as _:
        client = DagsterGraphQLClient("localhost")
        yield MockClient(
            python_client=client, mock_gql_client=client._client  # pylint: disable=W0212
        )


def test_get_run_status_success(mock_client):
    expected_result = "SUCCESS"
    response = {"pipelineRunOrError": {"__typename": "PipelineRun", "status": expected_result}}
    mock_client.mock_gql_client.execute.return_value = response

    actual_result = mock_client.python_client.get_run_status("foo")
    assert actual_result == expected_result


def test_get_run_status_fails_with_python_error(mock_client):
    error_type, error_msg = "PythonError", "something exploded"
    response = {"pipelineRunOrError": {"__typename": error_type, "message": error_msg}}
    mock_client.mock_gql_client.execute.return_value = response

    with pytest.raises(DagsterGraphQLClientError) as exc_info:
        mock_client.python_client.get_run_status("foo")

    assert exc_info.value.args == (error_type, error_msg)


def test_get_run_status_fails_with_pipeline_run_not_found_error(mock_client):
    error_type, error_msg = "PipelineRunNotFoundError", "The specified pipeline run does not exist"
    response = {"pipelineRunOrError": {"__typename": error_type, "message": error_msg}}
    mock_client.mock_gql_client.execute.return_value = response

    with pytest.raises(DagsterGraphQLClientError) as exc_info:
        mock_client.python_client.get_run_status("foo")

    assert exc_info.value.args == (error_type, error_msg)


def test_get_run_status_fails_with_query_error(mock_client):
    mock_client.mock_gql_client.execute.side_effect = Exception("foo")

    with pytest.raises(DagsterGraphQLClientError) as _:
        mock_client.python_client.get_run_status("foo")


def test_reload_repo_location_success(mock_client):
    response = {"reloadRepositoryLocation": {"__typename": "RepositoryLocation"}}
    mock_client.mock_gql_client.execute.return_value = response

    assert (
        mock_client.python_client.reload_repository_location("foo").status
        == ReloadRepositoryLocationStatus.SUCCESS
    )


def test_reload_repo_location_failure(mock_client):
    error_msg = "some reason"
    response = {
        "reloadRepositoryLocation": {
            "__typename": "RepositoryLocationLoadFailure",
            "error": {"message": error_msg},
        }
    }
    mock_client.mock_gql_client.execute.return_value = response

    result = mock_client.python_client.reload_repository_location("foo")
    assert result.status == ReloadRepositoryLocationStatus.FAILURE
    assert result.message == error_msg


def test_reload_repo_location_fails_with_query_error(mock_client):
    mock_client.mock_gql_client.execute.side_effect = Exception("foo")

    with pytest.raises(DagsterGraphQLClientError) as _:
        mock_client.python_client.reload_repository_location("foo")
