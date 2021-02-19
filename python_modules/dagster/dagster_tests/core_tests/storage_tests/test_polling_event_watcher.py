import tempfile
import time
from contextlib import contextmanager

from dagster.core.events import DagsterEvent, DagsterEventType, EngineEventData
from dagster.core.events.log import EventRecord
from dagster.core.storage.event_log import SqlitePollingEventLogStorage

RUN_ID = "foo"


def create_event(count: int, run_id: str = RUN_ID):
    return EventRecord(
        None,
        str(count),
        "debug",
        "",
        run_id,
        time.time(),
        dagster_event=DagsterEvent(
            DagsterEventType.ENGINE_EVENT.value,
            "nonce",
            event_specific_data=EngineEventData.in_process(999),
        ),
    )


@contextmanager
def create_sqlite_run_event_logstorage():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        yield SqlitePollingEventLogStorage(tmpdir_path)


def test_using_logstorage():
    with create_sqlite_run_event_logstorage() as storage:
        watched_1 = []
        watched_2 = []

        assert len(storage.get_logs_for_run(RUN_ID)) == 0

        storage.store_event(create_event(1))
        assert len(storage.get_logs_for_run(RUN_ID)) == 1
        assert len(watched_1) == 0

        storage.watch(RUN_ID, 0, watched_1.append)

        storage.store_event(create_event(2))
        storage.store_event(create_event(3))

        storage.watch(RUN_ID, 2, watched_2.append)
        storage.store_event(create_event(4))

        attempts = 10
        while len(watched_1) < 3 and len(watched_2) < 1 and attempts > 0:
            time.sleep(0.1)
            attempts -= 1

        assert len(storage.get_logs_for_run(RUN_ID)) == 4
        assert len(watched_1) == 3
        assert len(watched_2) == 1

        storage.end_watch(RUN_ID, watched_1.append)
        time.sleep(0.3)  # this value scientifically selected from a range of attractive values
        storage.store_event(create_event(5))

        attempts = 10
        while len(watched_2) < 2 and attempts > 0:
            time.sleep(0.1)
            attempts -= 1
        storage.end_watch(RUN_ID, watched_2.append)

        assert len(storage.get_logs_for_run(RUN_ID)) == 5
        assert len(watched_1) == 3
        assert len(watched_2) == 2

        storage.delete_events(RUN_ID)

        assert len(storage.get_logs_for_run(RUN_ID)) == 0
        assert len(watched_1) == 3
        assert len(watched_2) == 2

        assert [int(evt.message) for evt in watched_1] == [2, 3, 4]
        assert [int(evt.message) for evt in watched_2] == [4, 5]
