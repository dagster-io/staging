import time
from contextlib import contextmanager

import pytest
from dagster import seven
from dagster.core.storage.event_log import ConsolidatedSqliteEventLogStorage, SqliteEventLogStorage


@contextmanager
def create_sqlite_run_event_logstorage():
    with seven.TemporaryDirectory() as tmpdir_path:
        yield SqliteEventLogStorage(tmpdir_path)


@contextmanager
def create_consolidated_sqlite_run_event_log_storage():
    with seven.TemporaryDirectory() as tmpdir_path:
        yield ConsolidatedSqliteEventLogStorage(tmpdir_path)


heartbeat_storage_test = pytest.mark.parametrize(
    "heartbeat_storage_factory_cm_fn",
    [create_sqlite_run_event_logstorage, create_consolidated_sqlite_run_event_log_storage,],
)


@heartbeat_storage_test
def test_empty_heartbeat(heartbeat_storage_factory_cm_fn):
    with heartbeat_storage_factory_cm_fn() as storage:
        assert storage.daemon_healthy() is False


@heartbeat_storage_test
def test_add_heartbeat(heartbeat_storage_factory_cm_fn):
    with heartbeat_storage_factory_cm_fn() as storage:
        storage.add_daemon_heartbeat(current_time_seconds=1000)
        assert storage.daemon_healthy(current_time_seconds=1001) is True
        assert storage.daemon_healthy(current_time_seconds=2000) is False
