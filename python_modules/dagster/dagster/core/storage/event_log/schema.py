import sqlalchemy as db
from sqlalchemy.ext.compiler import compiles

## TODO: @compiles directive? and type-subclassing

# how many decimal places should dates be represented to in MySQL? can go from 0-6
MYSQL_DATE_PRECISION: int = 6

# datetime issue: fix here: https://stackoverflow.com/questions/29711102/sqlalchemy-mysql-millisecond-or-microsecond-precision/29723278
@compiles(db.DateTime, "mysql")
def compile_datetime_and_add_precision_mysql(type_, compiler, **kw):  # pylint:disable=W0613
    return f"DATETIME({MYSQL_DATE_PRECISION})"


class get_current_timestamp(db.sql.expression.FunctionElement):
    type = db.types.DateTime()


@compiles(get_current_timestamp, "mysql")
def compiles_get_current_timestamp_mysql(element, compiler, **kw):  # pylint:disable=W0613
    return f"CURRENT_TIMESTAMP({MYSQL_DATE_PRECISION})"


@compiles(get_current_timestamp)
def compiles_get_current_timestamp_default(element, compiler, **kw):  # pylint:disable=W0613
    return "CURRENT_TIMESTAMP"


@compiles(db.types.TIMESTAMP, "mysql")
def add_precision_to_mysql_timestamps(element, compiler, **kw):  # pylint:disable=W0613
    return f"TIMESTAMP({MYSQL_DATE_PRECISION})"


# Custom String type that
class CustomString(db.String):
    """Custom String Type that maps to different types depending on DB API in use.

    Use as a hack to avoid migrations between major releases

    MySQL: Maps to db.String(UNIQUE_VARCHAR_LEN)
    Postgres: Maps to db.Text

    """

    class comparator_factory(db.String.Comparator):
        pass


@compiles(CustomString)
def compiles_custom_string_default(element, compiler, **kw):
    return compiler.visit_text(element, **kw)


UNIQUE_VARCHAR_LEN: int = 512


@compiles(CustomString, "mysql")
def compiles_custom_string_mysql(element, compiler, **kw):
    kw["type_expression"].type.length = UNIQUE_VARCHAR_LEN
    return compiler.visit_string(element, **kw)


SqlEventLogStorageMetadata = db.MetaData()

SqlEventLogStorageTable = db.Table(
    "event_logs",
    SqlEventLogStorageMetadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("run_id", db.String(255)),
    db.Column("event", db.Text, nullable=False),
    db.Column("dagster_event_type", db.Text),
    db.Column("timestamp", db.types.TIMESTAMP),
    db.Column("step_key", db.Text),
    db.Column("asset_key", db.Text),
    db.Column("partition", db.Text),
)

SecondaryIndexMigrationTable = db.Table(
    "secondary_indexes",
    SqlEventLogStorageMetadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("name", CustomString, unique=True),
    db.Column("create_timestamp", db.DateTime, server_default=get_current_timestamp()),
    db.Column("migration_completed", db.DateTime),
)

AssetKeyTable = db.Table(
    "asset_keys",
    SqlEventLogStorageMetadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("asset_key", CustomString, unique=True),
    db.Column("create_timestamp", db.DateTime, server_default=get_current_timestamp()),
)

db.Index("idx_run_id", SqlEventLogStorageTable.c.run_id)
db.Index("idx_step_key", SqlEventLogStorageTable.c.step_key)
db.Index("idx_asset_key", SqlEventLogStorageTable.c.asset_key)
db.Index(
    "idx_asset_partition", SqlEventLogStorageTable.c.asset_key, SqlEventLogStorageTable.c.partition
)
