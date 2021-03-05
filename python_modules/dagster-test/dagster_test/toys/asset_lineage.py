import datetime
import os
import random
import string

import pandas as pd
from dagster import (
    Array,
    AssetKey,
    EventMetadataEntry,
    Field,
    ModeDefinition,
    Output,
    OutputDefinition,
    Partition,
    PartitionSetDefinition,
    configured,
    pipeline,
    solid,
)
from dagster.core.storage.fs_io_manager import PickledObjectFilesystemIOManager
from dagster.core.storage.io_manager import io_manager


def get_date_partitions():
    """Every day in 2020"""
    d1 = datetime.date(2020, 1, 1)
    d2 = datetime.date(2021, 1, 1)
    days = [d1 + datetime.timedelta(days=x) for x in range((d2 - d1).days + 1)]

    return [Partition(day.strftime("%Y-%m-%d")) for day in days]


def run_config_for_date_partition(partition):
    date = partition.value

    return {
        "solids": {
            "download_data": {
                "outputs": {"result": {"table": "daily_raw_data", "partitions": [date]}}
            },
            "split_action_types": {
                "outputs": {
                    "comments": {"table": "daily_raw_comments", "partitions": [date]},
                    "stories": {"table": "daily_raw_stories", "partitions": [date]},
                }
            },
            "top_10_comments": {
                "outputs": {"result": {"table": "daily_top_comments", "partitions": [date]}}
            },
            "top_10_stories": {
                "outputs": {"result": {"table": "daily_top_stories", "partitions": [date]}}
            },
            "top_10_actions": {
                "outputs": {"result": {"table": "daily_top_actions", "partitions": [date]}}
            },
        }
    }


asset_lineage_partition_set = PartitionSetDefinition(
    name="date_partition_set",
    pipeline_name="asset_lineage_pipeline",
    partition_fn=get_date_partitions,
    run_config_fn_for_partition=run_config_for_date_partition,
)


def metadata_for_actions(df):
    return [
        EventMetadataEntry.int(int(df["score"].min()), "min score"),
        EventMetadataEntry.int(int(df["score"].max()), "max score"),
        EventMetadataEntry.md(df[:5].to_markdown(), "sample rows"),
    ]


class MyDatabaseIOManager(PickledObjectFilesystemIOManager):
    def _get_path(self, context):
        keys = context.get_run_scoped_output_identifier()

        return os.path.join("/tmp", *keys)

    def handle_output(self, context, obj):
        super().handle_output(context, obj)
        # can pretend this actually came from a library call
        yield EventMetadataEntry.int(len(obj), "num rows written to db")

    def get_output_asset_key(self, context):
        return AssetKey(
            [
                "my_database",
                context.metadata["table_name"],
            ]
        )

    def get_output_asset_partitions(self, context):
        return set(context.config.get("partitions", []))


@io_manager(output_config_schema={Field(Array(str), is_required=False)})
def my_db_io_manager(_):
    return MyDatabaseIOManager()


@solid(
    output_defs=[
        OutputDefinition(io_manager_key="my_db_io_manager"),
    ],
)
def download_data(_):
    n_entries = random.randint(100, 1000)

    def user_id():
        return "".join(random.choices(string.ascii_uppercase, k=10))

    # generate some random data
    data = {
        "user_id": [user_id() for i in range(n_entries)],
        "action_type": [
            random.choices(["story", "comment"], [0.15, 0.85])[0] for i in range(n_entries)
        ],
        "score": [random.randint(0, 10000) for i in range(n_entries)],
    }
    df = pd.DataFrame.from_dict(data)
    yield Output(df, metadata_entries=metadata_for_actions(df))


@solid(
    output_defs=[
        OutputDefinition(
            name="stories", io_manager_key="my_db_io_manager", metadata={"table_name": "stories"}
        ),
        OutputDefinition(
            name="comments", io_manager_key="my_db_io_manager", metadata={"table_name": "comments"}
        ),
    ]
)
def split_action_types(_, df):

    stories_df = df[df["action_type"] == "story"]
    comments_df = df[df["action_type"] == "comment"]
    yield Output(
        stories_df,
        "stories",
        metadata_entries=metadata_for_actions(stories_df),
    )
    yield Output(comments_df, "comments", metadata_entries=metadata_for_actions(comments_df))


@solid(config_schema={"n": int}, output_defs=[OutputDefinition(io_manager_key="my_db_io_manager")])
def best_n_actions(context, df):
    df = df.nlargest(context.solid_config["n"], "score")
    return Output(
        df,
        metadata_entries=[
            EventMetadataEntry.md(df.to_markdown(), "data"),
        ],
    )


top_10_comments = configured(best_n_actions, name="top_10_comments")({"n": 10})
top_10_stories = configured(best_n_actions, name="top_10_stories")({"n": 10})
top_10_actions = configured(best_n_actions, name="top_10_actions")({"n": 10})


@solid
def combine_dfs(_, df1, df2):
    return pd.concat([df1, df2])


@pipeline(mode_defs=[ModeDefinition(resource_defs={"my_db_io_manager": my_db_io_manager})])
def asset_lineage_pipeline():
    stories, comments = split_action_types(download_data())
    top_10_actions(combine_dfs(top_10_stories(stories), top_10_comments(comments)))
