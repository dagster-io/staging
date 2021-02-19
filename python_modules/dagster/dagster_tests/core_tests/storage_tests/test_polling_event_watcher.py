import tempfile
import time
from contextlib import contextmanager

from dagster.core.events import DagsterEvent, DagsterEventType, EngineEventData
from dagster.core.events.log import EventRecord
from dagster.core.storage.event_log import SqlPollingEventWatcher, SqliteEventLogStorage


class SqlEventLogStorageWithPolling(SqliteEventLogStorage):
    """Test class that uses the SqlPollingEventWatcher instead of a filesystem/OS-based method"""

    def __init__(self, base_dir: str):
        super(SqlEventLogStorageWithPolling, self).__init__(base_dir)
        self._watcher = SqlPollingEventWatcher(self)

    def watch(self, run_id, start_cursor, callback):
        self._watcher.watch_run(run_id, start_cursor, callback)

    def end_watch(self, run_id, handler):
        self._watcher.unwatch_run(run_id, handler)


@contextmanager
def create_sqlite_run_event_logstorage():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        yield SqlEventLogStorageWithPolling(tmpdir_path)


def test_polling_event_watcher():
    def evt(name):
        return EventRecord(
            None,
            name,
            "debug",
            "",
            "foo",
            time.time(),
            dagster_event=DagsterEvent(
                DagsterEventType.ENGINE_EVENT.value,
                "nonce",
                event_specific_data=EngineEventData.in_process(999),
            ),
        )

    with create_sqlite_run_event_logstorage() as storage:
        watched = []
        watcher = lambda x: watched.append(x)  # pylint: disable=unnecessary-lambda

        assert len(storage.get_logs_for_run("foo")) == 0

        storage.store_event(evt("Message1"))
        assert len(storage.get_logs_for_run("foo")) == 1
        assert len(watched) == 0

        storage.watch("foo", 1, watcher)

        storage.store_event(evt("Message2"))
        storage.store_event(evt("Message3"))
        storage.store_event(evt("Message4"))

        attempts = 10
        while len(watched) < 3 and attempts > 0:
            time.sleep(0.1)
            attempts -= 1

        assert len(storage.get_logs_for_run("foo")) == 4
        assert len(watched) == 3

        storage.end_watch("foo", watcher)
        time.sleep(0.3)  # this value scientifically selected from a range of attractive values
        storage.store_event(evt("Message5"))

        assert len(storage.get_logs_for_run("foo")) == 5
        assert len(watched) == 3

        storage.delete_events("foo")

        assert len(storage.get_logs_for_run("foo")) == 0
        assert len(watched) == 3
