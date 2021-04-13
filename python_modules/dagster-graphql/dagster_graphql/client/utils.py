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


class RepositoryNode(NamedTuple):
    repository_location_name: str
    repository_names: List[str]

    @staticmethod
    def from_dict(obj: Dict[str, Any]):
        repository_names: List[str] = [repository["name"] for repository in obj["repositories"]]
        return RepositoryNode(
            repository_location_name=obj["name"], repository_names=repository_names
        )
