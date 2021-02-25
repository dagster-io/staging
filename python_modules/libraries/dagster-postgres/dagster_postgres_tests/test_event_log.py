import time

import pytest
import yaml
from dagster.core.storage.event_log.schema import AssetKeyTable, SqlEventLogStorageTable
from dagster.core.test_utils import instance_for_test
from dagster_postgres.event_log import PostgresEventLogStorage
from dagster_tests.core_tests.storage_tests.utils.event_log_storage import (
    RUN_ID,
    TestEventLogStorage,
    create_event,
)


class TestPostgresEventLogStorage(TestEventLogStorage):
    __test__ = True

    @pytest.fixture(scope="function", name="storage")
    def event_log_storage(self, conn_string):  # pylint: disable=arguments-differ
        storage = PostgresEventLogStorage.create_clean_storage(conn_string)
        assert storage
        yield storage
        # need to drop tables since PG tables are not run-sharded & some tests depend on a totally
        # fresh table (i.e.) autoincr. ids starting at 1 - this is related to cursor API, see
        # https://github.com/dagster-io/dagster/issues/3621
        with storage.run_connection(run_id=None) as conn:
            SqlEventLogStorageTable.drop(conn)
            AssetKeyTable.drop(conn)

    @pytest.mark.skip("https://github.com/dagster-io/dagster/issues/3744")
    def test_event_log_storage_two_watchers(self, storage):
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
        while (len(watched_1) < 3 or len(watched_2) < 1) and attempts > 0:
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

    @pytest.mark.skip("https://github.com/dagster-io/dagster/issues/3744")
    def test_event_watcher_single_run_event(self, storage):
        pass

    @pytest.mark.skip("https://github.com/dagster-io/dagster/issues/3744")
    def test_event_watcher_filter_two_runs_event(self, storage):
        pass

    def test_load_from_config(self, hostname):
        url_cfg = """
        event_log_storage:
            module: dagster_postgres.event_log
            class: PostgresEventLogStorage
            config:
                postgres_url: postgresql://test:test@{hostname}:5432/test
        """.format(
            hostname=hostname
        )

        explicit_cfg = """
        event_log_storage:
            module: dagster_postgres.event_log
            class: PostgresEventLogStorage
            config:
                postgres_db:
                    username: test
                    password: test
                    hostname: {hostname}
                    db_name: test
        """.format(
            hostname=hostname
        )

        # pylint: disable=protected-access
        with instance_for_test(overrides=yaml.safe_load(url_cfg)) as from_url_instance:
            from_url = from_url_instance._event_storage

            with instance_for_test(overrides=yaml.safe_load(explicit_cfg)) as explicit_instance:
                from_explicit = explicit_instance._event_storage

                assert from_url.postgres_url == from_explicit.postgres_url
