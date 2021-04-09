import textwrap
from contextlib import contextmanager

from dagster import (
    AssetKey,
    EventMetadataEntry,
    Field,
    InputContext,
    OutputContext,
    StringSource,
    io_manager,
)
from dagster.core.storage.io_manager import IOManager
from pandas import DataFrame, read_sql
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL  # pylint: disable=no-name-in-module,import-error
from sqlalchemy import create_engine

SNOWFLAKE_CONFIG_SCHEMA = {
    "account": StringSource,
    "user": StringSource,
    "password": StringSource,
    "database": StringSource,
    "warehouse": StringSource,
}


@contextmanager
def connect_snowflake(config, schema="public"):
    for key, value in config.items():
        if not value:
            raise ValueError(f"Missing value for snowflake config key {key}")

    url = URL(
        account=config["account"],
        user=config["user"],
        password=config["password"],
        database=config["database"],
        warehouse=config["warehouse"],
        schema=schema,
        timezone="UTC",
    )

    conn = None
    try:
        conn = create_engine(url).connect()
        yield conn
    finally:
        if conn:
            conn.close()


@io_manager(
    config_schema=dict(
        **SNOWFLAKE_CONFIG_SCHEMA,
        **{
            "start_ts": Field(float, is_required=False),
            "end_ts": Field(float, is_required=False),
        },
    ),
)
def snowflake_io_manager(_):
    return SnowflakeIOManager()


class SnowflakeIOManager(IOManager):
    def get_output_asset_key(self, context: OutputContext):
        schema, table = context.metadata["table"].split(".")
        return AssetKey(["snowflake", schema, table])

    def handle_output(self, context: OutputContext, obj: DataFrame):
        from snowflake import connector  # pylint: disable=no-name-in-module

        yield EventMetadataEntry.int(obj.shape[0], "Rows")
        yield EventMetadataEntry.md(columns_to_markdown(obj), "DataFrame columns")

        connector.paramstyle = "pyformat"
        schema, table = context.metadata["table"].split(".")

        with connect_snowflake(config=context.resource_config, schema=schema) as con:
            with_uppercase_cols = obj.rename(str.upper, copy=False, axis="columns")
            with_uppercase_cols.to_sql(
                table,
                con=con,
                if_exists="replace",
                index=False,
                method=pd_writer,
                schema=schema.upper(),
            )

    def load_input(self, context: InputContext) -> DataFrame:
        with connect_snowflake(config=context.resource_config) as con:
            result = read_sql(
                sql=f'select * from {context.upstream_output.metadata["table"]}', con=con
            )
            result.columns = map(str.lower, result.columns)
            return result


def columns_to_markdown(dataframe: DataFrame) -> str:
    return (
        textwrap.dedent(
            """
        | Name | Type |
        | ---- | ---- |
    """
        )
        + "\n".join([f"| {name} | {dtype}" for name, dtype in dataframe.dtypes.iteritems()])
    )
