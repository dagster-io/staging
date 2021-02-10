import sqlalchemy as db
from dagster import check
from sqlalchemy.ext.compiler import compiles

# Includes SQLAlchemy types, compiler directives, etc. to avoid pre-0.11.0 migrations
# as well as compiler directives to make cross-DB API semantics the same.

# 1: make MySQL dates equivalent to PG or Sqlite dates

MYSQL_DATE_PRECISION: int = 6
check.invariant(
    0 <= MYSQL_DATE_PRECISION <= 6,
    "`MYSQL_DATE_PRECISION` represents the floating point precision of MySQL dates. It can range from 0 to 6 (inclusive)",
)

# datetime issue fix from here: https://stackoverflow.com/questions/29711102/sqlalchemy-mysql-millisecond-or-microsecond-precision/29723278
@compiles(db.DateTime, "mysql")
def compile_datetime_and_add_precision_mysql(_element, _compiler, **_kw):
    return f"DATETIME({MYSQL_DATE_PRECISION})"


class get_current_timestamp(db.sql.expression.FunctionElement):
    """Like CURRENT_TIMESTAMP, but has the same semantics on MySQL, Postgres, and Sqlite"""

    type = db.types.DateTime()


@compiles(get_current_timestamp, "mysql")
def compiles_get_current_timestamp_mysql(_element, _compiler, **_kw):
    return f"CURRENT_TIMESTAMP({MYSQL_DATE_PRECISION})"


@compiles(get_current_timestamp)
def compiles_get_current_timestamp_default(_element, _compiler, **_kw):
    return "CURRENT_TIMESTAMP"


@compiles(db.types.TIMESTAMP, "mysql")
def add_precision_to_mysql_timestamps(_element, _compiler, **_kw):
    return f"TIMESTAMP({MYSQL_DATE_PRECISION})"


# 2: Custom String type to avoid mandatory migration for existing users until 0.11.0 release
# Should stop being used after the 0.11.0 release (although it might be helpful for future migration issues)

UNIQUE_VARCHAR_LEN: int = 512


class CustomString(db.String):
    """Custom String Type that maps to different types depending on DB API in use.

    Use as a hack to avoid migrations between major releases

    1. MySQL :: db.String(UNIQUE_VARCHAR_LEN)
    2. Postgres :: db.Text
    3. Sqlite :: db.Text

    Maps to db.Text by default

    """

    # so python code can treat this as a normal string
    class comparator_factory(db.String.Comparator):
        pass


@compiles(CustomString)
def compiles_custom_string_default(element, compiler, **kw):
    return compiler.visit_text(element, **kw)


@compiles(CustomString, "mysql")
def compiles_custom_string_mysql(element, compiler, **kw):
    kw["type_expression"].type.length = UNIQUE_VARCHAR_LEN
    return compiler.visit_string(element, **kw)
