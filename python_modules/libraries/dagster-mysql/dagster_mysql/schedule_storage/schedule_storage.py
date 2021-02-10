import sqlalchemy as db
from dagster import check
from dagster.core.storage.schedules import ScheduleStorageSqlMetadata, SqlScheduleStorage
from dagster.core.storage.sql import create_engine, get_alembic_config, run_alembic_upgrade
from dagster.serdes import ConfigurableClass, ConfigurableClassData

from ..utils import (
    create_mysql_connection,
    mysql_config,
    mysql_url_from_config,
    retry_mysql_creation_fn,
)


class MySQLScheduleStorage(SqlScheduleStorage, ConfigurableClass):
    """MySQL-backed run storage.

    Users should not directly instantiate this class; it is instantiated by internal machinery when
    ``dagit`` and ``dagster-graphql`` load, based on the values in the ``dagster.yaml`` file in
    ``$DAGSTER_HOME``. Configuration of this class should be done by setting values in that file.

    To use MySQL for schedule storage, you can add a block such as the following to your
    ``dagster.yaml``:

    .. literalinclude:: ../../../../../examples/docs_snippets/docs_snippets/deploying/dagster-pg.yaml
       :caption: dagster.yaml
       :lines: 23-32
       :language: YAML

    Note that the fields in this config are :py:class:`~dagster.StringSource` and
    :py:class:`~dagster.IntSource` and can be configured from environment variables.
    """

    def __init__(self, mysql_url, inst_data=None):
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)
        self.mysql_url = mysql_url

        # Default to not holding any connections open to prevent accumulating connections per DagsterInstance
        self._engine = create_engine(
            self.mysql_url, isolation_level="AUTOCOMMIT", poolclass=db.pool.NullPool
        )

        with self.connect() as conn:
            retry_mysql_creation_fn(lambda: ScheduleStorageSqlMetadata.create_all(conn))

    def optimize_for_dagit(self, statement_timeout):
        # When running in dagit, hold an open connection and set statement_timeout
        self._engine = create_engine(
            self.mysql_url,
            isolation_level="AUTOCOMMIT",
            pool_size=1,
        )

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return mysql_config()

    @staticmethod
    def from_config_value(inst_data, config_value):
        return MySQLScheduleStorage(
            inst_data=inst_data, mysql_url=mysql_url_from_config(config_value)
        )

    @staticmethod
    def create_clean_storage(mysql_url):
        engine = create_engine(mysql_url, isolation_level="AUTOCOMMIT", poolclass=db.pool.NullPool)
        try:
            ScheduleStorageSqlMetadata.drop_all(engine)
        finally:
            engine.dispose()
        return MySQLScheduleStorage(mysql_url)

    def connect(self, run_id=None):  # pylint: disable=arguments-differ, unused-argument
        return create_mysql_connection(self._engine, __file__, "schedule")

    def upgrade(self):
        alembic_config = get_alembic_config(__file__)
        run_alembic_upgrade(alembic_config, self._engine)
