from typing import Callable

from dagster import check
from dagster.core.events.log import EventRecord

from .. import SqlPollingEventWatchdog
from . import SqliteEventLogStorage


class SqlitePollingEventLogStorage(SqliteEventLogStorage):
    """Replaces SqliteEventLogStorage's SqliteEventLogStorageWatchdog with SqlPollingEventWatcher"""

    def __init__(self, *args, **kwargs):
        super(SqlitePollingEventLogStorage, self).__init__(*args, **kwargs)
        self._watcher = SqlPollingEventWatchdog(self)

    @staticmethod
    def from_config_value(inst_data, config_value):
        return SqlitePollingEventLogStorage(inst_data=inst_data, **config_value)

    def watch(self, run_id: str, start_cursor: int, callback: Callable[[EventRecord], None]):
        check.str_param(run_id, "run_id")
        check.int_param(start_cursor, "start_cursor")
        check.callable_param(callback, "callback")
        self._watcher.watch_run(run_id, start_cursor, callback)

    def end_watch(self, run_id: str, handler: Callable[[EventRecord], None]):
        check.str_param(run_id, "run_id")
        check.callable_param(handler, "handler")
        self._watcher.unwatch_run(run_id, handler)
