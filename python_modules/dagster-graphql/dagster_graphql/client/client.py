from typing import Any, Dict, Optional

import requests
from dagster import check
from dagster.core.storage.pipeline_run import PipelineRunStatus

from .query import (
    GET_PIPELINE_STATUS_QUERY,
    LAUNCH_PIPELINE_EXECUTION_MUTATION,
    RELOAD_REPOSITORY_LOCATION_MUTATION,
)


class DagsterGraphQLClient:
    """Official Dagster Python Client for GraphQL

    Note that everything method is currently blocking / syncronous
    """

    def __init__(self, hostname: str, port_number: Optional[int] = None):
        self._hostname = check.str_param(hostname, "hostname")
        self._port_number = check.opt_int_param(port_number, "port_number")
        self.url = (
            f"{self._hostname}:{self._port_number}" if self._port_number else self._hostname
        ) + "/graphql"
        super(DagsterGraphQLClient, self).__init__()

    def _execute(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[Any, Any]:
        """Executes the query/mutation with associated variables against a GraphQL server via HTTP request
             ** this function is blocking **

        Args:
            query (str): The GraphQL query / mutation to execute
            variables (Dict[str, Any], optional): Dict of variables relevant to the query.
                Defaults to {}.

        Returns:
            Dict[Any, Any]: JSON of request result
        """
        json = {"query": query}
        if variables is not None:
            json["variables"] = variables
        return requests.post(self.url, json=json).json()

    def submit_pipeline_execution(
        self,
        repository_location_name: Optional[str],
        repository_name: Optional[str],
        pipeline_name: str,
        run_config_data: Optional[Any] = None,
        mode: Optional[str] = None,
        preset_name: Optional[str] = None,
    ) -> str:
        """Submits a pipeline to the RunCoordinator? for execution

        Can either set all of repo_loc + repo_name + pipeline_name or just pipeline_name

        Can also either configure a preset or mode + runConfigData

        """
        check.opt_str_param(repository_location_name, "repository_location_name")
        check.opt_str_param(repository_name, "repository_name")
        check.str_param(pipeline_name, "pipeline_name")
        check.opt_str_param(mode, "mode")
        check.opt_str_param(preset_name, "preset_name")
        variables = {
            "executionParams": {
                "selector": {
                    "repositoryLocationName": repository_location_name,
                    "repositoryName": repository_name,
                    "pipelineName": pipeline_name,
                }
            }
        }
        if mode is not None and run_config_data is not None:
            variables["executionParams"] = {
                **variables["executionParams"],
                "runConfigData": run_config_data,
                "mode": mode,
            }
        elif preset_name is not None:
            variables["executionParams"]["preset"] = preset_name
        else:
            raise Exception("must specify mode + runConfigData or a preset for the pipeline")

        res_data = self._execute(LAUNCH_PIPELINE_EXECUTION_MUTATION, variables)
        query_result = res_data["data"]["launchPipelineExecution"]
        if query_result["__typename"] == "LaunchPipelineRunSuccess":
            return query_result["run"]["runId"]
        else:
            raise Exception("something failed in query execution")

    def get_run_status(self, run_id: str) -> PipelineRunStatus:
        check.str_param(run_id, "run_id")
        res_data = self._execute(GET_PIPELINE_STATUS_QUERY, {"runId": run_id})
        query_result = res_data["data"]["pipelineRunOrError"]
        if query_result["__typename"] == "PipelineRun":
            return query_result["status"]
        else:
            raise Exception("foo")

    def reload_repository_location(self, repository_location_name: str) -> bool:
        check.str_param(repository_location_name, "repository_location_name")
        res_data = self._execute(
            RELOAD_REPOSITORY_LOCATION_MUTATION,
            {"repositoryLocationName": repository_location_name},
        )
        return res_data["data"]["reloadRepositoryLocation"]["__typename"] == "RepositoryLocation"
