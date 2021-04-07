from enum import Enum
from typing import NamedTuple, Optional


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