import threading
from collections import namedtuple
from typing import Callable, List, MutableMapping, NamedTuple

import sqlalchemy as db
from dagster import check
from dagster.core.events.log import EventRecord
from dagster.core.storage.event_log import (
    AssetAwareSqlEventLogStorage,
    SqlEventLogStorageMetadata,
    SqlEventLogStorageTable,
)
from dagster.core.storage.sql import create_engine, get_alembic_config, run_alembic_upgrade
from dagster.serdes import (
    ConfigurableClass,
    ConfigurableClassData,
    deserialize_json_to_dagster_namedtuple,
)

from ..utils import (
    create_mysql_connection,
    mysql_config,
    mysql_statement_timeout,
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

        with self.connect() as conn:
            retry_mysql_creation_fn(lambda: SqlEventLogStorageMetadata.create_all(conn))

    def optimize_for_dagit(self, statement_timeout):
        # When running in dagit, hold an open connection and set statement_timeout
        # TODO: statement_timeout doesn't seem to exist in mysql. Alternatives?
        self._engine = create_engine(self.mysql_url, isolation_level="AUTOCOMMIT", pool_size=1)

    def upgrade(self):
        alembic_config = get_alembic_config(__file__)
        run_alembic_upgrade(alembic_config, self._engine)

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
        with self.connect() as conn:
            res = conn.execute(insert_event_statement)
            # event_id = res.inserted_primary_key
            res.close()
            # TODO
            # conn.execute(
            #     """NOTIFY {channel}, %s; """.format(channel=CHANNEL_NAME),
            #     (event.run_id + "_" + str(id),),
            # )
            if event.is_dagster_event and event.dagster_event.asset_key:
                self.store_asset_key(conn, event)

    def store_asset_key(self, conn, event):
        check.inst_param(event, "event", EventRecord)
        if not event.is_dagster_event or not event.dagster_event.asset_key:
            return

        # TODO
        # conn.execute(
        #     db.dialects.mysql.insert(AssetKeyTable)
        #     .values(asset_key=event.dagster_event.asset_key.to_string())
        #     .on_conflict_do_nothing(index_elements=[AssetKeyTable.c.asset_key])
        # )

    def connect(self, run_id=None):
        return create_mysql_connection(self._engine, __file__, "event log")

    def has_secondary_index(self, name, run_id=None):
        if name not in self._secondary_index_cache:
            self._secondary_index_cache[name] = super(  # pylint: disable=E1121
                MySQLEventLogStorage, self
            ).has_secondary_index(name, run_id)
        return self._secondary_index_cache[name]

    def enable_secondary_index(self, name, run_id=None):
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


EventWatcherProcessStartedEvent = namedtuple("EventWatcherProcessStartedEvent", "")
EventWatcherStart = namedtuple("EventWatcherStart", "")
EventWatcherEvent = namedtuple("EventWatcherEvent", "payload")
EventWatchFailed = namedtuple("EventWatchFailed", "message")
EventWatcherEnd = namedtuple("EventWatcherEnd", "")

EventWatcherThreadEvents = (
    EventWatcherProcessStartedEvent,
    EventWatcherStart,
    EventWatcherEvent,
    EventWatchFailed,
    EventWatcherEnd,
)
EventWatcherThreadNoopEvents = (EventWatcherProcessStartedEvent, EventWatcherStart)
EventWatcherThreadEndEvents = (EventWatchFailed, EventWatcherEnd)

POLLING_CADENCE = 0.25

TERMINATE_EVENT_LOOP = "TERMINATE_EVENT_LOOP"


class CallbackAfterCursor(NamedTuple):
    start_cursor: int
    callback: Callable[[EventRecord], None]


class MySQLEventWatcher:
    '''MySQL-based Event Log Watcher; uses one thread per watched run_id
    '''

    def __init__(self, conn_str: str):
        # TODO: should this be held in the MySQLEventLogStorage class? probably.
        # look into a fn call similar to 'optimize_for_dagit'
        check.str_param(conn_str, "conn_str")
        self._engine: db.engine.Engine = create_engine(conn_str, isolation_level="AUTOCOMMIT")

        # INVARIANT: dict_lock protects _run_id_to_watcher_dict
        self._dict_lock: threading.Lock = threading.Lock()
        self._run_id_to_watcher_dict: MutableMapping[str, MySQLRunIdEventWatcherThread] = {}

    def has_run_id(self, run_id: str) -> bool:
        run_id = check.str_param(run_id, "run_id")
        with self._dict_lock:
            _has_run_id = run_id in self._run_id_to_watcher_dict
        return _has_run_id

    def watch_run(self, run_id: str, start_cursor: int, callback: Callable[[EventRecord], None]):
        run_id = check.str_param(run_id, "run_id")
        start_cursor = check.int_param(start_cursor, "start_cursor")
        callback = check.callable_param(callback, "callback")
        with self._dict_lock:
            if run_id not in self._run_id_to_watcher_dict:
                self._run_id_to_watcher_dict[run_id] = MySQLRunIdEventWatcherThread(
                    self._engine, run_id
                )
                self._run_id_to_watcher_dict[run_id].daemon = True
                self._run_id_to_watcher_dict[run_id].start()

            self._run_id_to_watcher_dict[run_id].add_callback(start_cursor, callback)

    def unwatch_run(self, run_id: str, handler: Callable[[EventRecord], None]):
        run_id = check.str_param(run_id, "run_id")
        handler = check.callable_param(handler, "handler")
        with self._dict_lock:
            if run_id in self._run_id_to_watcher_dict:
                self._run_id_to_watcher_dict[run_id].remove_callback(handler)
                if self._run_id_to_watcher_dict[run_id].should_thread_exit.is_set():
                    del self._run_id_to_watcher_dict[run_id]

    def close(self):
        with self._dict_lock:
            for watcher_thread in self._run_id_to_watcher_dict:
                watcher_thread.should_thread_exit.set()
            for watcher_thread in self._run_id_to_watcher_dict:
                watcher_thread.join()


class MySQLRunIdEventWatcherThread(threading.Thread):
    '''subclass of Thread that watches a given run_id for new Events in a MySQL DB.

    Exits when `self.should_thread_exit` is set.

    '''

    def __init__(self, engine: db.engine.Engine, run_id: str):
        super(threading.Thread, self).__init__()  # pylint: disable=E1003
        self._engine = check.inst_param(engine, "engine", db.engine.Engine)
        self._run_id = check.str_param(run_id, "run_id")
        self._callback_fn_list: List[CallbackAfterCursor] = []
        self._should_thread_exit = threading.Event()
        self.name = f"mysql-event-watch-run-id-{self._run_id}"

    @property
    def should_thread_exit(self) -> threading.Event:
        return self._should_thread_exit

    def add_callback(self, start_event_id_cursor: int, callback: Callable[[EventRecord], None]):
        '''Add a callback to execute on event_ids > start_event_id_cursor

        Args:
            start_event_id_cursor (int): minimum event_id for the callback to execute
            callback (Callable[[EventRecord], None]): callback to update the Dagster UI
        '''
        start_event_id_cursor = check.int_param(start_event_id_cursor, "start_event_id_cursor")
        callback = check.callable_param(callback, "callback")
        self._callback_fn_list.append(CallbackAfterCursor(start_event_id_cursor, callback))

    def remove_callback(self, callback: Callable[[EventRecord], None]):
        '''Remove a callback from the list of callbacks to execute on new Events

        Also stop tracking run id if appropriate

        Args:
            callback (Callable[[EventRecord], None]): callback to remove from list of callbacks
        '''
        callback = check.callable_param(callback, "callback")
        self._callback_fn_list = [
            callback_with_cursor
            for callback_with_cursor in self._callback_fn_list
            if callback_with_cursor.callback != callback
        ]
        if not self._callback_fn_list:
            # no Observers remaining
            self._should_thread_exit.set()

    def run(self):
        with self._engine.connect() as conn:
            res: db.engine.ResultProxy = conn.execute(
                db.select([SqlEventLogStorageTable.c.id, SqlEventLogStorageTable.c.event]).where(
                    SqlEventLogStorageTable.c.run_id == self._run_id
                )
            )
            while not self._should_thread_exit.wait(POLLING_CADENCE):
                for record in res.fetchall():
                    # TODO: this isn't right, new data won't be fetched within calls
                    # however, a cursor-based approach seems like the right approach here.
                    # I'll just modify the query for now (greater ids since the primary key is autoincremented)
                    index: int = record[0]
                    dagster_event: EventRecord = deserialize_json_to_dagster_namedtuple(record[1])
                    for callback_with_cursor in self._callback_fn_list:
                        if callback_with_cursor.start_cursor <= index:
                            callback_with_cursor.callback(dagster_event)
