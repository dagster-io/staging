import tempfile
import time
from collections import namedtuple
from contextlib import contextmanager
from unittest.mock import Mock

import pytest
from dagster.core.events import DagsterEvent, DagsterEventType, EngineEventData
from dagster.core.events.log import EventRecord
from dagster.core.storage.event_log import SqlEventLogStorage, SqlitePollingEventLogStorage
from dagster.core.test_utils import instance_for_test_tempdir
from dagster_graphql.implementation.pipeline_run_storage import PipelineRunObservableSubscribe


@contextmanager
def create_test_instance_and_storage():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        with instance_for_test_tempdir(
            tmpdir_path,
            overrides={
                "event_log_storage": {
                    "module": "dagster.core.storage.event_log",
                    "class": "SqlitePollingEventLogStorage",
                    "config": {"base_dir": tmpdir_path},
                }
            },
        ) as instance:
            yield (instance, instance._event_storage)  # pylint: disable=protected-access


RUN_ID = "foo"


class EventStorer:
    def __init__(self, storage: SqlEventLogStorage):
        self._storage = storage
        self._counter = 0

    def store_n_events(self, n: int):
        for _ in range(n):
            self._counter += 1
            self._storage.store_event(self.create_event(self._counter))

    @staticmethod
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


NEventsAndCursor = namedtuple("NEventsAndCursor", ["num_events_before_watch", "after_cursor"])

NUM_EVENTS_BEFORE_WATCH = 3
NUM_EVENTS_AFTER_WATCH = 3


@pytest.mark.parametrize(
    "before_watch_config",
    [
        NEventsAndCursor(num_events_before_watch, after_cursor)
        for num_events_before_watch in range(0, NUM_EVENTS_BEFORE_WATCH + 1)
        for after_cursor in range(-1, num_events_before_watch + 1)
    ],
)
@pytest.mark.parametrize("num_events_after_watch", list(range(1, NUM_EVENTS_AFTER_WATCH + 1)))
def test_using_instance(before_watch_config: NEventsAndCursor, num_events_after_watch: int):
    with create_test_instance_and_storage() as (instance, storage):
        assert isinstance(storage, SqlitePollingEventLogStorage)
        observable_subscribe = PipelineRunObservableSubscribe(
            instance, RUN_ID, after_cursor=before_watch_config.after_cursor
        )
        event_storer = EventStorer(storage)
        event_storer.store_n_events(before_watch_config.num_events_before_watch)
        observable_subscribe(Mock())
        event_storer.store_n_events(num_events_after_watch)
        time.sleep(0.5)
        call_args = observable_subscribe.observer.on_next.call_args_list
        events_list = [[event_record.message for event_record in call[0][0]] for call in call_args]
        flattened_events_list = [int(message) for lst in events_list for message in lst]
        beginning_cursor = before_watch_config.after_cursor + 2
        assert flattened_events_list == list(
            range(
                beginning_cursor,
                before_watch_config.num_events_before_watch + num_events_after_watch + 1,
            )
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

        storage.store_event(EventStorer.create_event(1))
        assert len(storage.get_logs_for_run(RUN_ID)) == 1
        assert len(watched_1) == 0

        storage.watch(RUN_ID, 0, watched_1.append)

        storage.store_event(EventStorer.create_event(2))
        storage.store_event(EventStorer.create_event(3))

        storage.watch(RUN_ID, 2, watched_2.append)
        storage.store_event(EventStorer.create_event(4))

        attempts = 10
        while len(watched_1) < 3 and len(watched_2) < 1 and attempts > 0:
            time.sleep(0.1)
            attempts -= 1

        assert len(storage.get_logs_for_run(RUN_ID)) == 4
        assert len(watched_1) == 3
        assert len(watched_2) == 1

        storage.end_watch(RUN_ID, watched_1.append)
        time.sleep(0.3)  # this value scientifically selected from a range of attractive values
        storage.store_event(EventStorer.create_event(5))

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
