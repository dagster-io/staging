from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import NamedTuple, Optional

from dagster.core.instance import MayHaveInstanceWeakref


class CapturedLogData(
    NamedTuple(
        "_CapturedLogData",
        [("chunk", Optional[bytes]), ("cursor", Optional[int])],
    )
):
    """
    Object representing captured log data, either a partial chunk of the log data or the full
    capture.  Contains the raw bytes and optionally the cursor offset for the partial chunk.
    """

    def __new__(cls, chunk: Optional[bytes] = None, cursor: Optional[int] = None):
        return super(CapturedLogData, cls).__new__(cls, chunk, cursor)


class CapturedLogMetadata(
    NamedTuple(
        "_CapturedLogMetadata",
        [("location", Optional[str]), ("download_url", Optional[str])],
    )
):
    """
    Object representing metadata info for the captured log data, containing a display string for
    the location of the log data and a URL for direct download of the captured log data.
    """

    def __new__(cls, location: Optional[str] = None, download_url: Optional[str] = None):
        return super(CapturedLogMetadata, cls).__new__(cls, location, download_url)


class CapturedLogManager(ABC, MayHaveInstanceWeakref):
    """Abstract base class for capturing the unstructured compute logs (stdout/stderr) in the
    current process, stored / retrieved with a provided log_key and namespace."""

    @abstractmethod
    @contextmanager
    def capture_logs(self, log_key: str, namespace: Optional[str] = None):
        pass

    @abstractmethod
    def is_capture_complete(self, log_key: str, namespace: Optional[str] = None):
        pass

    @abstractmethod
    def get_stdout(
        self,
        log_key: str,
        namespace: Optional[str] = None,
        cursor: str = None,
        max_bytes: int = None,
    ) -> CapturedLogData:
        pass

    @abstractmethod
    def get_stderr(
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

    def should_capture_run_by_step(self) -> bool:
        return False
