import random

from dagster import (
    AssetKey,
    EventMetadataEntry,
    Float,
    InputDefinition,
    Output,
    OutputDefinition,
    String,
    pipeline,
    solid,
)
from dagster.core.definitions.events import AssetPartitions, PartitionSpecificMetadataEntry


def db_connection():
    class Connection:
        def execute(self, _query):
            return random.randint(0, 100)

    return Connection()


@solid(
    config_schema={"table_name": str, "partition": str},
    output_defs=[
        OutputDefinition(
            name="table_name",
            dagster_type=String,
            asset_fn=lambda context: [
                AssetPartitions(AssetKey("table"), partitions=[context.solid_config["partition"]])
            ],
        ),
        OutputDefinition(name="some_float", dagster_type=Float),
    ],
)
def solid1(context):
    con = db_connection()
    table_name = context.solid_config["table_name"]
    partition = context.solid_config["partition"]
    con.execute(
        f"""
        delete from table {table_name}
        where partition = {partition}
        """
    )
    con.execute(
        f"""
        insert into {table_name}
        select * from source_table
        where partition = {partition}
        """
    )
    nrows = con.execute(
        f"""
        select COUNT(*) from {table_name}
        where partition = {partition}
        """
    )
    yield Output(
        table_name,
        "table_name",
        metadata_entries=[
            EventMetadataEntry.text("my_table", "output table name"),
            EventMetadataEntry.int(nrows, "number of rows"),
            EventMetadataEntry.int(1234, "max value"),
            EventMetadataEntry.int(0, "min value"),
            PartitionSpecificMetadataEntry(
                context.solid_config["partition"],
                EventMetadataEntry.text("something interesting", "something interesting"),
            ),
        ],
    )
    my_float = 3.141592653589793238462
    yield Output(
        my_float, "some_float", metadata_entries=[EventMetadataEntry.float(my_float, "value")]
    )


@solid(
    input_defs=[InputDefinition("table_name", dagster_type=String)],
)
def solid2(_, table_name):
    con = db_connection()
    con.execute(
        f"""
        create table solid2 as
        select * from {table_name}
        where some_condition
        """
    )


@solid
def solid3(_, _some_float):
    pass


@pipeline
def output_metadata_pipeline():
    dataframe_val, float_val = solid1()
    solid2(dataframe_val)
    solid3(float_val)
