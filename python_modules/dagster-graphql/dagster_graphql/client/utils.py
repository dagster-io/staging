from enum import Enum
from typing import Any, Dict, List, NamedTuple, Optional


class DagsterGraphQLClientError(Exception):
    def __init__(self, *args, body=None):
        super(DagsterGraphQLClientError, self).__init__(*args)
        self.body = body


class ReloadRepositoryLocationStatus(Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class ReloadRepositoryLocationInfo(NamedTuple):
    status: ReloadRepositoryLocationStatus
    message: Optional[str] = None


class PipelineInfo(NamedTuple):
    repository_location_name: str
    repository_name: str
    pipeline_name: str

    @staticmethod
    def from_node(node: Dict[str, Any]) -> List["PipelineInfo"]:
        repo_name = node["name"]
        repo_location_name = node["location"]["name"]
        return [
            PipelineInfo(
                repository_location_name=repo_location_name,
                repository_name=repo_name,
                pipeline_name=pipeline["name"],
            )
            for pipeline in node["pipelines"]
        ]
