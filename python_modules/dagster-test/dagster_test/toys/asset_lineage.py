import os
import pickle
import random
import string

import pandas as pd
from dagster import (
    AssetKey,
    EventMetadataEntry,
    ModeDefinition,
    Output,
    OutputDefinition,
    check,
    pipeline,
    solid,
)
from dagster.core.execution.context.system import InputContext, OutputContext
from dagster.core.storage.io_manager import IOManager, io_manager
from dagster.utils import PICKLE_PROTOCOL, mkdir_p


def make_dummy_data():
    pass


class RewrittenPOFIOManager(IOManager):
    """
    It should be possible to replace the current implementation of the FS IOManagers with this,
    as they have the same properties in the default case. For now, I'm just putting it here.
    """

    def __init__(self, base_dir=None):
        self.base_dir = check.opt_str_param(base_dir, "base_dir")
        self.write_mode = "wb"
        self.read_mode = "rb"

    def _get_path(self, context):
        keys = context.get_run_scoped_output_identifier()

        return os.path.join(self.base_dir, *keys)

    def handle_output(self, context, obj):
        check.inst_param(context, "context", OutputContext)

        filepath = self._get_path(context)

        # Ensure path exists
        mkdir_p(os.path.dirname(filepath))
        context.log.debug(f"Writing file at: {filepath}")

        with open(filepath, self.write_mode) as write_obj:
            pickle.dump(obj, write_obj, PICKLE_PROTOCOL)

        yield EventMetadataEntry.fspath(os.path.abspath(filepath))

    def load_input(self, context):
        """Unpickle the file from a given file path and Load it to a data object."""
        check.inst_param(context, "context", InputContext)
        filepath = self._get_path(context.upstream_output)
        context.log.debug(f"Loading file from: {filepath}")

        with open(filepath, self.read_mode) as read_obj:
            return pickle.load(read_obj)

    def get_output_asset_key(self, context):
        # I'm not sure I like the default AssetKey path here
        return AssetKey([context.pipeline_name, context.step_key, context.name])


@io_manager
def fs_io_manager(_):
    return RewrittenPOFIOManager(base_dir="/tmp")


@solid(
    output_defs=[
        OutputDefinition(name="left_handed", io_manager_key="fs_io_manager"),
        OutputDefinition(name="right_handed", io_manager_key="fs_io_manager"),
    ]
)
def load_data(_):
    n_entries = random.randint(10, 80)

    def user_id():
        return "".join(random.choices(string.ascii_uppercase, k=10))

    data = {
        "name": [user_id() for i in range(n_entries)],
        "is_right_handed": [
            random.choices([True, False], [0.85, 0.15])[0] for i in range(n_entries)
        ],
        "attribute": [random.randint(0, 1000) for i in range(n_entries)],
    }
    df = pd.DataFrame.from_dict(data)
    left_df = df[df["is_right_handed"] == False]
    right_df = df[df["is_right_handed"] == True]
    yield Output(
        left_df,
        "left_handed",
        metadata_entries=[EventMetadataEntry.int(len(left_df), "nrows")],
    )
    yield Output(
        right_df,
        "right_handed",
        metadata_entries=[EventMetadataEntry.int(len(right_df), "nrows")],
    )


@solid(
    config_schema={"n": int},
    output_defs=[OutputDefinition(name="highest_n_attribute", io_manager_key="fs_io_manager")],
)
def highest_n_attribute(context, df):
    df = df.nlargest(context.solid_config["n"], "attribute")
    return Output(
        df,
        "highest_n_attribute",
        metadata_entries=[
            EventMetadataEntry.int(int(df["attribute"].min()), "min value"),
            EventMetadataEntry.int(int(df["attribute"].max()), "max value"),
        ],
    )


@solid(output_defs=[OutputDefinition(io_manager_key="fs_io_manager")])
def combine_dfs(_, df1, df2):
    return pd.concat([df1, df2])


@pipeline(mode_defs=[ModeDefinition(resource_defs={"fs_io_manager": fs_io_manager})])
def asset_lineage_pipeline():
    left_handed, right_handed = load_data()
    highest_n_left = highest_n_attribute.alias("left_n")(left_handed)
    highest_n_right = highest_n_attribute.alias("right_n")(right_handed)
    combine_dfs(highest_n_left, highest_n_right)
