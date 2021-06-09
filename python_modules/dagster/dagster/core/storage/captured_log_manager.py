from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import NamedTuple, Optional

from dagster import check
from dagster.core.instance import MayHaveInstanceWeakref


class CapturedLogData(
    NamedTuple(
        "_CapturedLogData",
        [("data", Optional[bytes]), ("cursor", Optional[int])],
    )
):
    def __new__(cls, data=None, cursor=None):
        check.opt_bytes_param(data, "data")
        check.opt_int_param(cursor, "cursor")
        return super(CapturedLogData, cls).__new__(cls, data, cursor)


class CapturedLogMetadata(
    NamedTuple(
        "_CapturedLogMetadata",
        [("location", Optional[str]), ("download_url", Optional[str])],
    )
):
    def __new__(cls, location=None, download_url=None):
        check.opt_str_param(location, "location")
        check.opt_str_param(download_url, "download_url")
        return super(CapturedLogMetadata, cls).__new__(cls, location, download_url)


class CapturedLogManager(ABC, MayHaveInstanceWeakref):
    """Abstract base class for storing unstructured compute logs (stdout/stderr) from the compute
    steps of pipeline solids."""

    @abstractmethod
    @contextmanager
    def capture_logs(self, log_key: str, namespace: Optional[str] = None):
        pass

    @abstractmethod
    def is_capture_complete(self, log_key: str, namespace: Optional[str] = None):
        pass

    @abstractmethod
    def read_stdout(
        self,
        log_key: str,
        namespace: Optional[str] = None,
        cursor: str = None,
        max_bytes: int = None,
    ) -> CapturedLogData:
        pass

    @abstractmethod
    def read_stderr(
        self,
        log_key: str,
        namespace: Optional[str] = None,
        cursor: str = None,
        max_bytes: int = None,
    ) -> CapturedLogData:
        pass

    @abstractmethod
    def get_stdout_metadata(
        self, log_key: str, namespace: Optional[str] = None
    ) -> CapturedLogMetadata:
        pass

    @abstractmethod
    def get_stderr_metadata(
        self, log_key: str, namespace: Optional[str] = None
    ) -> CapturedLogMetadata:
        pass

    def is_enabled(self, _log_key: str, _namespace: Optional[str] = None):
        return True

    def should_capture_run_by_step(self) -> bool:
        return False
