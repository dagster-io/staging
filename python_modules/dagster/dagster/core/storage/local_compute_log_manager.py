import hashlib
import os
import sys
from collections import defaultdict
from contextlib import contextmanager
from typing import Optional

from dagster import Field, Float, StringSource, check
from dagster.core.execution.compute_logs import mirror_stream_to_file
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.serdes import ConfigurableClass, ConfigurableClassData
from dagster.utils import ensure_dir, touch_file
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers.polling import PollingObserver

from .captured_log_manager import CapturedLogData, CapturedLogManager, CapturedLogMetadata
from .compute_log_manager import (
    MAX_BYTES_FILE_READ,
    ComputeIOType,
    ComputeLogFileData,
    ComputeLogManager,
    ComputeLogSubscription,
)

DEFAULT_WATCHDOG_POLLING_TIMEOUT = 2.5

IO_TYPE_EXTENSION = {ComputeIOType.STDOUT: "out", ComputeIOType.STDERR: "err"}

MAX_FILENAME_LENGTH = 255


class LocalComputeLogManager(ComputeLogManager, CapturedLogManager, ConfigurableClass):
    """Stores copies of stdout & stderr for each compute step locally on disk."""

    def __init__(self, base_dir, polling_timeout=None, inst_data=None, capture_by_step=None):
        self._base_dir = base_dir
        self._polling_timeout = check.opt_float_param(
            polling_timeout, "polling_timeout", DEFAULT_WATCHDOG_POLLING_TIMEOUT
        )
        self._subscription_manager = LocalComputeLogSubscriptionManager(self)
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)
        self._capture_by_step = check.opt_bool_param(capture_by_step, "capture_by_step")

    @contextmanager
    def capture_logs(self, log_key: str, namespace: Optional[str] = None):
        outpath = self.get_local_path(namespace, log_key, ComputeIOType.STDOUT)
        errpath = self.get_local_path(namespace, log_key, ComputeIOType.STDERR)
        with mirror_stream_to_file(sys.stdout, outpath):
            with mirror_stream_to_file(sys.stderr, errpath):
                yield

        # leave artifact on filesystem so that we know the capture is completed
        touch_file(self.complete_artifact_path(namespace, log_key))

    def is_capture_complete(self, log_key: str, namespace: Optional[str] = None):
        return os.path.exists(self.complete_artifact_path(namespace, log_key))

    def _read_path(self, path: str, cursor: Optional[str] = None, max_bytes: Optional[int] = None):
        if not os.path.exists(path) or not os.path.isfile(path):
            return CapturedLogData()

        with open(path, "rb") as f:
            offset = 0
            if cursor:
                try:
                    offset = int(cursor)
                except ValueError:
                    pass
            f.seek(offset, os.SEEK_SET)
            if max_bytes is None:
                data = f.read()
            else:
                data = f.read(max_bytes)
            new_offset = f.tell()

        return CapturedLogData(data=data, cursor=new_offset)

    def read_stdout(
        self,
        log_key: str,
        namespace: Optional[str] = None,
        cursor: str = None,
        max_bytes: int = None,
    ) -> CapturedLogData:
        path = self.get_local_path(namespace, log_key, ComputeIOType.STDOUT)
        return self._read_path(path, cursor=cursor, max_bytes=max_bytes)

    def read_stderr(
        self,
        log_key: str,
        namespace: Optional[str] = None,
        cursor: str = None,
        max_bytes: int = None,
    ) -> CapturedLogData:
        path = self.get_local_path(namespace, log_key, ComputeIOType.STDERR)
        return self._read_path(path, cursor=cursor, max_bytes=max_bytes)

    def get_stdout_metadata(
        self, log_key: str, namespace: Optional[str] = None
    ) -> CapturedLogMetadata:
        return CapturedLogMetadata(
            location=self.get_local_path(namespace, log_key, ComputeIOType.STDOUT),
            download_url=self.download_url(namespace, log_key, ComputeIOType.STDOUT),
        )

    def get_stderr_metadata(
        self, log_key: str, namespace: Optional[str] = None
    ) -> CapturedLogMetadata:
        return CapturedLogMetadata(
            location=self.get_local_path(namespace, log_key, ComputeIOType.STDERR),
            download_url=self.download_url(namespace, log_key, ComputeIOType.STDERR),
        )

    def should_capture_run_by_step(self) -> bool:
        return self._capture_by_step

    @contextmanager
    def _watch_logs(self, pipeline_run, step_key=None):
        check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
        check.opt_str_param(step_key, "step_key")

        namespace = pipeline_run.run_id
        key = self.get_key(pipeline_run, step_key)
        with self.capture_logs(key, namespace):
            yield

    @property
    def inst_data(self):
        return self._inst_data

    @property
    def polling_timeout(self):
        return self._polling_timeout

    @classmethod
    def config_type(cls):
        return {
            "base_dir": StringSource,
            "polling_timeout": Field(Float, is_required=False),
            "capture_by_step": Field(bool, is_required=False, default_value=True),
        }

    @staticmethod
    def from_config_value(inst_data, config_value):
        return LocalComputeLogManager(inst_data=inst_data, **config_value)

    def _run_directory(self, run_id):
        if run_id:
            return os.path.join(self._base_dir, run_id, "compute_logs")
        else:
            return os.path.join(self._base_dir, "compute_logs")

    def get_local_path(self, run_id, key, io_type):
        check.inst_param(io_type, "io_type", ComputeIOType)
        return self._get_local_path(run_id, key, IO_TYPE_EXTENSION[io_type])

    def complete_artifact_path(self, run_id, key):
        return self._get_local_path(run_id, key, "complete")

    def _get_local_path(self, run_id, key, extension):
        filename = "{}.{}".format(key, extension)
        if len(filename) > MAX_FILENAME_LENGTH:
            filename = "{}.{}".format(hashlib.md5(key.encode("utf-8")).hexdigest(), extension)
        return os.path.join(self._run_directory(run_id), filename)

    def read_logs_file(self, run_id, key, io_type, cursor=0, max_bytes=MAX_BYTES_FILE_READ):
        path = self.get_local_path(run_id, key, io_type)

        if not os.path.exists(path) or not os.path.isfile(path):
            return ComputeLogFileData(path=path, data=None, cursor=0, size=0, download_url=None)

        # See: https://docs.python.org/2/library/stdtypes.html#file.tell for Windows behavior
        with open(path, "rb") as f:
            f.seek(cursor, os.SEEK_SET)
            data = f.read(max_bytes)
            cursor = f.tell()
            stats = os.fstat(f.fileno())

        # local download path
        download_url = self.download_url(run_id, key, io_type)
        return ComputeLogFileData(
            path=path,
            data=data.decode("utf-8"),
            cursor=cursor,
            size=stats.st_size,
            download_url=download_url,
        )

    def is_watch_completed(self, run_id, key):
        return self.is_capture_complete(key, run_id)

    def on_watch_start(self, pipeline_run, step_key):
        pass

    def get_key(self, pipeline_run, step_key):
        check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
        check.opt_str_param(step_key, "step_key")
        return step_key or pipeline_run.pipeline_name

    def on_watch_finish(self, pipeline_run, step_key=None):
        check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
        check.opt_str_param(step_key, "step_key")
        key = self.get_key(pipeline_run, step_key)
        touchpath = self.complete_artifact_path(pipeline_run.run_id, key)
        touch_file(touchpath)

    def download_url(self, run_id, key, io_type):
        check.inst_param(io_type, "io_type", ComputeIOType)
        return "/download/{}/{}/{}".format(run_id, key, io_type.value)

    def on_subscribe(self, subscription):
        self._subscription_manager.add_subscription(subscription)

    def on_unsubscribe(self, subscription):
        self._subscription_manager.remove_subscription(subscription)

    def dispose(self):
        self._subscription_manager.dispose()


class LocalComputeLogSubscriptionManager:
    def __init__(self, manager):
        self._manager = manager
        self._subscriptions = defaultdict(list)
        self._watchers = {}
        self._observer = None

    def _watch_key(self, run_id, key):
        return "{}:{}".format(run_id, key)

    def add_subscription(self, subscription):
        check.inst_param(subscription, "subscription", ComputeLogSubscription)
        if self._manager.is_watch_completed(subscription.run_id, subscription.key):
            subscription.fetch()
            subscription.complete()
        else:
            watch_key = self._watch_key(subscription.run_id, subscription.key)
            self._subscriptions[watch_key].append(subscription)
            self.watch(subscription.run_id, subscription.key)

    def remove_subscription(self, subscription):
        check.inst_param(subscription, "subscription", ComputeLogSubscription)
        watch_key = self._watch_key(subscription.run_id, subscription.key)
        if subscription in self._subscriptions[watch_key]:
            self._subscriptions[watch_key].remove(subscription)
            subscription.complete()

    def remove_all_subscriptions(self, run_id, step_key):
        watch_key = self._watch_key(run_id, step_key)
        for subscription in self._subscriptions.pop(watch_key, []):
            subscription.complete()

    def watch(self, run_id, step_key):
        watch_key = self._watch_key(run_id, step_key)
        if watch_key in self._watchers:
            return

        update_paths = [
            self._manager.get_local_path(run_id, step_key, ComputeIOType.STDOUT),
            self._manager.get_local_path(run_id, step_key, ComputeIOType.STDERR),
        ]
        complete_paths = [self._manager.complete_artifact_path(run_id, step_key)]
        directory = os.path.dirname(
            self._manager.get_local_path(run_id, step_key, ComputeIOType.STDERR)
        )

        if not self._observer:
            self._observer = PollingObserver(self._manager.polling_timeout)
            self._observer.start()

        ensure_dir(directory)

        self._watchers[watch_key] = self._observer.schedule(
            LocalComputeLogFilesystemEventHandler(
                self, run_id, step_key, update_paths, complete_paths
            ),
            str(directory),
        )

    def notify_subscriptions(self, run_id, step_key):
        watch_key = self._watch_key(run_id, step_key)
        for subscription in self._subscriptions[watch_key]:
            subscription.fetch()

    def unwatch(self, run_id, step_key, handler):
        watch_key = self._watch_key(run_id, step_key)
        if watch_key in self._watchers:
            self._observer.remove_handler_for_watch(handler, self._watchers[watch_key])
        del self._watchers[watch_key]

    def dispose(self):
        if self._observer:
            self._observer.stop()
            self._observer.join(15)


class LocalComputeLogFilesystemEventHandler(PatternMatchingEventHandler):
    def __init__(self, manager, run_id, key, update_paths, complete_paths):
        self.manager = manager
        self.run_id = run_id
        self.key = key
        self.update_paths = update_paths
        self.complete_paths = complete_paths
        patterns = update_paths + complete_paths
        super(LocalComputeLogFilesystemEventHandler, self).__init__(patterns=patterns)

    def on_created(self, event):
        if event.src_path in self.complete_paths:
            self.manager.remove_all_subscriptions(self.run_id, self.key)
            self.manager.unwatch(self.run_id, self.key, self)

    def on_modified(self, event):
        if event.src_path in self.update_paths:
            self.manager.notify_subscriptions(self.run_id, self.key)
