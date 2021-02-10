from typing import Callable

import sqlalchemy as db
from dagster import check
from dagster.core.events.log import EventRecord
from dagster.core.storage.event_log import AssetAwareSqlEventLogStorage, SqlEventLogStorageMetadata
from dagster.core.storage.sql import create_engine, get_alembic_config, run_alembic_upgrade
from dagster.serdes import ConfigurableClass, ConfigurableClassData

from ..utils import (
    create_mysql_connection,
    mysql_config,
    mysql_url_from_config,
    retry_mysql_creation_fn,
)

CHANNEL_NAME = "run_events"


class MySQLEventLogStorage(AssetAwareSqlEventLogStorage, ConfigurableClass):
    """MySQL-backed event log storage.

    Users should not directly instantiate this class; it is instantiated by internal machinery when
    ``dagit`` and ``dagster-graphql`` load, based on the values in the ``dagster.yaml`` file in
    ``$DAGSTER_HOME``. Configuration of this class should be done by setting values in that file.

    To use MySQL for event log storage, you can add a block such as the following to your
    ``dagster.yaml``:

    .. literalinclude:: ../../../../../examples/docs_snippets/docs_snippets/deploying/dagster-pg.yaml
       :caption: dagster.yaml
       :lines: 12-21
       :language: YAML

    Note that the fields in this config are :py:class:`~dagster.StringSource` and
    :py:class:`~dagster.IntSource` and can be configured from environment variables.

    """

    def __init__(self, mysql_url, inst_data=None):
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)
        self.mysql_url = check.str_param(mysql_url, "mysql_url")
        self._disposed = False

        self._event_watcher = MySQLEventWatcher(self.mysql_url)

        # Default to not holding any connections open to prevent accumulating connections per DagsterInstance
        self._engine = create_engine(
            self.mysql_url, isolation_level="AUTOCOMMIT", poolclass=db.pool.NullPool
        )
        self._secondary_index_cache = {}

        with self._connect() as conn:
            retry_mysql_creation_fn(lambda: SqlEventLogStorageMetadata.create_all(conn))

    def optimize_for_dagit(self, statement_timeout):
        # When running in dagit, hold an open connection and set statement_timeout
        # TODO: statement_timeout doesn't seem to exist in mysql. Alternatives?
        self._engine = create_engine(self.mysql_url, isolation_level="AUTOCOMMIT", pool_size=1)

    def upgrade(self):
        alembic_config = get_alembic_config(__file__)
        with self._connect() as conn:
            run_alembic_upgrade(alembic_config, conn)

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return mysql_config()

    @staticmethod
    def from_config_value(inst_data, config_value):
        return MySQLEventLogStorage(
            inst_data=inst_data, mysql_url=mysql_url_from_config(config_value)
        )

    @staticmethod
    def create_clean_storage(conn_string):
        inst = MySQLEventLogStorage(conn_string)
        inst.wipe()
        return inst

    def store_event(self, event):
        """Store an event corresponding to a pipeline run.
        Args:
            event (EventRecord): The event to store.
        """
        check.inst_param(event, "event", EventRecord)
        insert_event_statement = self.prepare_insert_event(event)  # from SqlEventLogStorage.py
        with self._connect() as conn:
            res = conn.execute(insert_event_statement)
            res.close()
        if event.is_dagster_event and event.dagster_event.asset_key:
            self.store_asset_key(event)

    def store_asset_key(self, event):
        check.inst_param(event, "event", EventRecord)
        if not event.is_dagster_event or not event.dagster_event.asset_key:
            return

        # TODO: Insert or do nothing on UNIQUE (asset_key) conflict

    def _connect(self):
        return create_mysql_connection(self._engine, __file__, "event log")

    def run_connection(self, run_id=None):
        return self._connect()

    def index_connection(self):
        return self._connect()

    def has_secondary_index(self, name):
        if name not in self._secondary_index_cache:
            self._secondary_index_cache[name] = super(
                MySQLEventLogStorage, self
            ).has_secondary_index(name)
        return self._secondary_index_cache[name]

    def enable_secondary_index(self, name):
        super(MySQLEventLogStorage, self).enable_secondary_index(name)
        if name in self._secondary_index_cache:
            del self._secondary_index_cache[name]

    def watch(self, run_id, start_cursor, callback):
        self._event_watcher.watch_run(run_id, start_cursor, callback)

    def end_watch(self, run_id, handler):
        self._event_watcher.unwatch_run(run_id, handler)

    @property
    def event_watcher(self):
        return self._event_watcher

    def __del__(self):
        # Keep the inherent limitations of __del__ in Python in mind!
        self.dispose()

    def dispose(self):
        if not self._disposed:
            self._disposed = True
            self._event_watcher.close()


POLLING_CADENCE = 0.1  # 100 ms


class MySQLEventWatcher:
    """MySQL-based Event Log Watcher"""

    def __init__(self, conn_str: str):
        # TODO: should _engine be held in the MySQLEventLogStorage class? probably.
        # look into a fn call similar to 'optimize_for_dagit'?
        pass

    def has_run_id(self, run_id: str) -> bool:
        pass

    def watch_run(self, run_id: str, start_cursor: int, callback: Callable[[EventRecord], None]):
        pass

    def unwatch_run(self, run_id: str, handler: Callable[[EventRecord], None]):
        pass

    def close(self):
        pass
