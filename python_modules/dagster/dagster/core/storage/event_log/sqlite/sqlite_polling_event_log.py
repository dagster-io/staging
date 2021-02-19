from typing import Callable

from dagster import check
from dagster.core.events.log import EventRecord

from .. import SqlPollingEventWatchdog
from . import SqliteEventLogStorage


class SqlitePollingEventLogStorage(SqliteEventLogStorage):
    """SQLite-backed event log storage that uses SqlPollingEventWatchdog for watching runs.

    This class is a subclass of SqliteEventLogStorage that uses the SqlPollingEventWatchdog class
    (polling via SELECT queries) instead of the SqliteEventLogStorageWatchdog (filesystem watcher) to
    observe runs.

    Users should not directly instantiate this class; it is instantiated by internal machinery when
    ``dagit`` and ``dagster-graphql`` load, based on the values in the ``dagster.yaml`` file in
    ``$DAGSTER_HOME``. Configuration of this class should be done by setting values in that file.

    To explicitly specify the SQLite Polling implementation for event log storage,
    you can add a block such as the following to your ``dagster.yaml``:

    .. code-block:: YAML

        run_storage:
          module: dagster.core.storage.event_log
          class: SqlitePollingEventLogStorage
          config:
            base_dir: /path/to/dir

    The ``base_dir`` param tells the event log storage where on disk to store the database.
    """

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
