import random
from contextlib import contextmanager

from dagster import (
    AssetMaterialization,
    EventMetadataEntry,
    IOManager,
    IOManagerDefinition,
    InputDefinition,
    ModeDefinition,
    Nothing,
    Output,
    OutputDefinition,
    io_manager,
    pipeline,
    solid,
)


def db_connection():
    class Connection:
        def execute(self, _query):
            return random.randint(0, 100)

    return Connection()


@solid(
    config_schema={"partition": str}, output_defs=[OutputDefinition(dagster_type=Nothing,)],
)
def solid1(context):
    con = db_connection()
    con.execute(
        f"""
        delete from table solid1
        where partition = {context.solid_config["partition"]}
        """
    )
    con.execute(
        f"""
        insert into solid1
        select * from source_table
        where partition = {context.solid_config["partition"]}
        """
    )
    nrows = con.execute(
        f"""
        select COUNT(*) from source_table
        where partition = {context.solid_config["partition"]}
        """
    )
    yield Output(None, metadata_entries=[EventMetadataEntry.int(nrows, "number of rows")])


@solid(
    output_defs=[OutputDefinition(dagster_type=Nothing)],
    input_defs=[InputDefinition("solid1", dagster_type=Nothing)],
)
def solid2(_):
    con = db_connection()
    con.execute(
        """
        create table solid2 as
        select * from solid1
        where some_condition
        """
    )


@pipeline
def output_metadata_pipeline():
    solid2(solid1())
