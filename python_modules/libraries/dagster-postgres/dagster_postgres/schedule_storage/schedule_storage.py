import sqlalchemy as db
from dagster import check
from dagster.core.errors import DagsterInstanceMigrationRequired
from dagster.core.storage.schedules import ScheduleStorageSqlMetadata, SqlScheduleStorage
from dagster.core.storage.sql import (
    check_alembic_revision,
    create_engine,
    get_alembic_config,
    run_alembic_upgrade,
    stamp_alembic_rev,
)
from dagster.serdes import ConfigurableClass, ConfigurableClassData

from ..utils import (
    create_pg_connection,
    pg_config,
    pg_statement_timeout,
    pg_url_from_config,
    retry_pg_creation_fn,
)


class PostgresScheduleStorage(SqlScheduleStorage, ConfigurableClass):
    """Postgres-backed run storage.

    Users should not directly instantiate this class; it is instantiated by internal machinery when
    ``dagit`` and ``dagster-graphql`` load, based on the values in the ``dagster.yaml`` file in
    ``$DAGSTER_HOME``. Configuration of this class should be done by setting values in that file.

    To use Postgres for schedule storage, you can add a block such as the following to your
    ``dagster.yaml``:

    .. literalinclude:: ../../../../../examples/docs_snippets/docs_snippets/deploying/dagster-pg.yaml
       :caption: dagster.yaml
       :lines: 23-32
       :language: YAML

    Note that the fields in this config are :py:class:`~dagster.StringSource` and
    :py:class:`~dagster.IntSource` and can be configured from environment variables.
    """

    def __init__(self, postgres_url, inst_data=None):
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)
        self.postgres_url = postgres_url

        # Default to not holding any connections open to prevent accumulating connections per DagsterInstance
        self._engine = create_engine(
            self.postgres_url, isolation_level="AUTOCOMMIT", poolclass=db.pool.NullPool
        )

        # Stamp the alembic revision as long as it isn't pre-0.10.0
        table_names = db.inspect(self._engine).get_table_names()
        should_stamp = (
            "schedules" not in table_names
            and "schedule_ticks" not in table_names
            and "jobs" not in table_names
            and "job_ticks" not in table_names
        )
        with self.connect() as conn:
            retry_pg_creation_fn(lambda: ScheduleStorageSqlMetadata.create_all(conn))

            alembic_config = get_alembic_config(__file__)
            db_revision, head_revision = check_alembic_revision(alembic_config, conn)
            if should_stamp and not (db_revision and head_revision):
                stamp_alembic_rev(alembic_config, self._engine)

    def optimize_for_dagit(self, statement_timeout):
        # When running in dagit, hold an open connection and set statement_timeout
        self._engine = create_engine(
            self.postgres_url,
            isolation_level="AUTOCOMMIT",
            pool_size=1,
            connect_args={"options": pg_statement_timeout(statement_timeout)},
        )

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return pg_config()

    @staticmethod
    def from_config_value(inst_data, config_value):
        return PostgresScheduleStorage(
            inst_data=inst_data, postgres_url=pg_url_from_config(config_value)
        )

    @staticmethod
    def create_clean_storage(postgres_url):
        engine = create_engine(
            postgres_url, isolation_level="AUTOCOMMIT", poolclass=db.pool.NullPool
        )
        try:
            ScheduleStorageSqlMetadata.drop_all(engine)
        finally:
            engine.dispose()
        return PostgresScheduleStorage(postgres_url)

    def connect(self, run_id=None):  # pylint: disable=arguments-differ, unused-argument
        return create_pg_connection(self._engine, __file__, "schedule")

    def upgrade(self):
        alembic_config = get_alembic_config(__file__)
        run_alembic_upgrade(alembic_config, self._engine)

    def check_for_migration(self):
        alembic_config = get_alembic_config(__file__)

        with self.connect() as conn:
            db_revision, head_revision = check_alembic_revision(alembic_config, conn)
            if db_revision != "b32a4f3036d2":
                raise DagsterInstanceMigrationRequired(
                    msg="Schedule storage is out of date",
                    db_revision=db_revision,
                    head_revision=head_revision,
                )
