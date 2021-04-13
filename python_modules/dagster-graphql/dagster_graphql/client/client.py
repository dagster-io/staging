from typing import Any, Dict, Optional

from dagster import check
from dagster.core.storage.pipeline_run import PipelineRunStatus
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

from .client_queries import (
    CLIENT_SUBMIT_PIPELINE_RUN_MUTATION,
    GET_PIPELINE_RUN_STATUS_QUERY,
    RELOAD_REPOSITORY_LOCATION_MUTATION,
)
from .utils import (
    DagsterGraphQLClientError,
    ReloadRepositoryLocationInfo,
    ReloadRepositoryLocationStatus,
)


class DagsterGraphQLClient:
    """Official Dagster Python Client for GraphQL

    Utilizes the gql library to dispatch queries over HTTP to a remote Dagster GraphQL Server

    As of now, all operations on this client are synchronous.

    Intended usage:

    .. code-block:: python

        client = DagsterGraphQLClient("localhost", port_number=3000)
        status = client.get_run_status(**SOME_RUN_ID**)

    """

    def __init__(self, hostname: str, port_number: Optional[int] = None):
        self._hostname = check.str_param(hostname, "hostname")
        self._port_number = check.opt_int_param(port_number, "port_number")
        self._url = (
            "http://"
            + (f"{self._hostname}:{self._port_number}" if self._port_number else self._hostname)
            + "/graphql"
        )
        self._transport = RequestsHTTPTransport(url=self._url, use_json=True)
        self._client = Client(transport=self._transport, fetch_schema_from_transport=True)
        super(DagsterGraphQLClient, self).__init__()

    def _execute(self, query: str, variables: Dict[str, Any]):
        try:
            return self._client.execute(gql(query), variable_values=variables)
        except Exception as exc:
            raise DagsterGraphQLClientError(
                f"Query \n{query}\n with variables \n{variables}\n failed GraphQL validation"
            ) from exc

    def submit_pipeline_execution(
        self,
        pipeline_name: str,
        repository_location_name: Optional[str] = None,
        repository_name: Optional[str] = None,
        run_config_data: Optional[Any] = None,
        mode: Optional[str] = None,
        preset_name: Optional[str] = None,
    ) -> str:
        """Submits a Pipeline (specified by repository_location_name + repo_name + pipeline_name)
            with attached configuration to the RunCoordinator? for execution

        Args:
            pipeline_name (str): [description]
            repository_location_name (Optional[str]): [description]. Defaults to None.
            repository_name (Optional[str]): [description]. Defaults to None.
            run_config_data (Optional[Any], optional): [description]. Defaults to None.
            mode (Optional[str], optional): [description]. Defaults to None.
            preset_name (Optional[str], optional): [description]. Defaults to None.

        Raises:
            DagsterGraphQLClientError: client errors can happen in several ways, including:
                1. An error from submitted params
                2. An error from the response:
                    a. InvalidStepError
                    b. InvalidOutputError
                    c. ConflictingExecutionParamsError
                    d. PresetNotFoundError
                    e. PipelineConfigurationInvalid
                    f. PipelineNotFoundError
                    g. PythonError

        Returns:
            str: run id of the submitted pipeline run
        """
        check.opt_str_param(repository_location_name, "repository_location_name")
        check.opt_str_param(repository_name, "repository_name")
        check.str_param(pipeline_name, "pipeline_name")
        check.opt_str_param(mode, "mode")
        check.opt_str_param(preset_name, "preset_name")
        check.invariant(
            (mode is not None and run_config_data is not None) or preset_name is not None,
            "Either a mode and run_config_data or a preset must be specified in order to"
            + f"submit the pipeline {pipeline_name} for execution",
        )

        variables = {
            "executionParams": {
                "selector": {
                    "repositoryLocationName": repository_location_name,
                    "repositoryName": repository_name,
                    "pipelineName": pipeline_name,
                }
            }
        }
        if preset_name is not None:
            variables["executionParams"]["preset"] = preset_name
        else:
            variables["executionParams"] = {
                **variables["executionParams"],
                "runConfigData": run_config_data,
                "mode": mode,
            }

        res_data: Dict[str, Any] = self._execute(CLIENT_SUBMIT_PIPELINE_RUN_MUTATION, variables)
        query_result = res_data["launchPipelineExecution"]
        query_result_type = query_result["__typename"]
        if query_result_type == "LaunchPipelineRunSuccess":
            return query_result["run"]["runId"]
        elif query_result_type == "InvalidStepError":
            raise DagsterGraphQLClientError(query_result_type, query_result["invalidStepKey"])
        elif query_result_type == "InvalidOutputError":
            relevant_error_properties = frozenset({"stepKey", "invalidOutputName"})
            raise DagsterGraphQLClientError(
                query_result_type,
                {key: query_result.get(key, None) for key in relevant_error_properties},
            )
        elif query_result_type == "PipelineConfigurationInvalid":
            raise DagsterGraphQLClientError(query_result_type, query_result["errors"])
        else:
            # query_result_type is a ConflictingExecutionParamsError, a PresetNotFoundError
            # or a PipelineNotFoundError, or a PythonError
            raise DagsterGraphQLClientError(query_result_type, query_result["message"])

    def get_run_status(self, run_id: str) -> PipelineRunStatus:
        """Get the status of a given Pipeline Run

        Args:
            run_id (str): run id of the requested pipeline run.

        Raises:
            DagsterGraphQLClientError: raises an error with a message of:
                1. PipelineNotFoundError if the requested run id is not found
                2. PythonError on internal framework errors

        Returns:
            PipelineRunStatus: returns a status Enum describing the state of the
                requested pipeline run
        """
        check.str_param(run_id, "run_id")
        res_data: Dict[str, Dict[str, Any]] = self._execute(
            GET_PIPELINE_RUN_STATUS_QUERY, {"runId": run_id}
        )
        query_result: Dict[str, Any] = res_data["pipelineRunOrError"]
        query_result_type: str = query_result["__typename"]
        if query_result_type == "PipelineRun":
            return query_result["status"]
        else:
            raise DagsterGraphQLClientError(query_result_type, query_result["message"])

    def reload_repository_location(
        self, repository_location_name: str
    ) -> ReloadRepositoryLocationInfo:
        """Reloads a Dagster Repository Location

            This is useful in a variety of contexts, including refreshing Dagit without restarting
            the server.

        Args:
            repository_location_name (str): The name of the repository location

        Returns:
            ReloadRepositoryLocationInfo:
                1. with a `.status` attribute of `ReloadRepositoryLocationStatus.SUCCESS` if successful
                2. with a `.status` attribute of `ReloadRepositoryLocationStatus.FAILURE` if the reload failed,
                    and a `.message` attribute with the attached error message
        """
        check.str_param(repository_location_name, "repository_location_name")
        res_data: Dict[str, Dict[str, Any]] = self._execute(
            RELOAD_REPOSITORY_LOCATION_MUTATION,
            {"repositoryLocationName": repository_location_name},
        )
        query_result: Dict[str, Any] = res_data["reloadRepositoryLocation"]
        query_result_type: str = query_result["__typename"]
        if query_result_type == "RepositoryLocation":
            return ReloadRepositoryLocationInfo(status=ReloadRepositoryLocationStatus.SUCCESS)
        else:
            return ReloadRepositoryLocationInfo(
                status=ReloadRepositoryLocationStatus.FAILURE,
                message=query_result["error"]["message"],
            )
