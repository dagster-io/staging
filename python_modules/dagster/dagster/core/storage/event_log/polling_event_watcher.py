import threading
from typing import Callable

from dagster.core.events.log import EventRecord

from .sql_event_log import SqlEventLogStorage

POLLING_CADENCE = 0.1  # 100 ms


class SqlPollingEventWatcher:
    """Event Log Watcher that uses a multithreaded polling approach to retrieving new events for run_ids"""

    def __init__(self, event_log_storage: SqlEventLogStorage):
        pass

    def has_run_id(self, run_id: str) -> bool:
        pass

    def watch_run(self, run_id: str, start_cursor: int, callback: Callable[[EventRecord], None]):
        pass

    def unwatch_run(self, run_id: str, handler: Callable[[EventRecord], None]):
        pass

    def close(self):
        pass


class SqlPollingRunIdEventWatcherThread(threading.Thread):
    """subclass of Thread that watches a given run_id for new Events by polling every POLLING_CADENCE"""

    @property
    def should_thread_exit(self) -> threading.Event:
        pass

    def add_callback(self, start_cursor: int, callback: Callable[[EventRecord], None]):
        pass

    def remove_callback(self, callback: Callable[[EventRecord], None]):
        pass

    def run(self):
        pass
