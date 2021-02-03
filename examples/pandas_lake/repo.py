import sqlite3

import numpy as np
from dagster import (
    IOManager,
    InputDefinition,
    ModeDefinition,
    OutputDefinition,
    io_manager,
    pipeline,
    root_input_manager,
    solid,
)
from pandas import read_sql


class SqliteIOManager(IOManager):
    def handle_output(self, context, obj):
        table_name = context.metadata["table_name"]
        with sqlite3.connect(context.resource_config["db_path"]) as con:
            obj.to_sql(table_name, con, if_exists="replace")

    def load_input(self, context):
        table_name = context.upstream_output.metadata["table_name"]
        with sqlite3.connect(context.resource_config["db_path"]) as con:
            return read_sql(f"select * from {table_name}", con)


@io_manager(config_schema={"db_path": str})
def sqlite_io_manager(_):
    return SqliteIOManager()


@root_input_manager(config_schema={"db_path": str})
def sqlite_root_manager(context):
    table_name = context.metadata["table_name"]
    with sqlite3.connect(context.resource_config["db_path"]) as con:
        return read_sql(f"select * from {table_name}", con)


@solid(
    input_defs=[
        InputDefinition(
            "people", root_manager_key="root_manager", metadata={"table_name": "people"}
        )
    ],
    output_defs=[OutputDefinition(metadata={"table_name": "people_age_brackets"})],
)
def compute_people_age_brackets(_, people):
    """Buckets people based on their age."""
    result = people[["name"]].copy()
    result["age_bracket"] = np.where(people["age"] > 50, "> 50", "<= 50")
    return result


@solid(output_defs=[OutputDefinition(metadata={"table_name": "age_bracket_counts"})])
def compute_age_bracket_counts(_, people):
    """Counts the number of people in each age bracket."""
    return people.groupby("age_bracket", as_index=False)["name"].count()


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={"io_manager": sqlite_io_manager, "root_manager": sqlite_root_manager},
        ),
    ]
)
def my_pipeline():
    compute_age_bracket_counts(compute_people_age_brackets())
