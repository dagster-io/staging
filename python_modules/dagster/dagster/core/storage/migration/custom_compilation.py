import sqlalchemy as db
from sqlalchemy.ext.compiler import compiles

# Includes SQLAlchemy types, compiler directives, etc. to avoid pre-0.11.0 migrations
# as well as compiler directives to make cross-DB API semantics the same.

# 1: make MySQL dates equivalent to PG or Sqlite dates

MYSQL_DATE_PRECISION: int = 6

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


# Should stop being used after the 0.11.0 release (although it might be helpful for future migration issues)

UNIQUE_VARCHAR_LEN: int = 512


@compiles(db.Text, "mysql")
def compiles_text_mysql(element, compiler, **kw):
    if kw["type_expression"].unique:
        kw["type_expression"].type.length = UNIQUE_VARCHAR_LEN
        return compiler.visit_string(element, **kw)
    else:
        return compiler.visit_text(element, **kw)
