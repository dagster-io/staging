import pytest
import yaml
from dagster.core.storage.event_log.schema import AssetKeyTable, SqlEventLogStorageTable
from dagster.core.test_utils import instance_for_test
from dagster_postgres.event_log import PostgresEventLogStorage
from dagster_tests.core_tests.storage_tests.utils.event_log_storage import TestEventLogStorage


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
    def test_event_log_storage_watch(self, storage):
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
