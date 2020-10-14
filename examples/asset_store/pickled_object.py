from dagster import (
    DagsterInstance,
    ModeDefinition,
    OutputDefinition,
    PresetDefinition,
    execute_pipeline,
    pipeline,
    reexecute_pipeline,
    solid,
)
from dagster.core.definitions.utils import struct_to_string
from dagster.core.storage.address_store import AddressStore
from dagster.core.storage.asset_store import default_filesystem_asset_store


def train(df):
    return len(df)


local_asset_store = default_filesystem_asset_store.configured(
    {"base_dir": "uncommitted/intermediates/"}
)


@solid(
    output_defs=[
        OutputDefinition(
            asset_store_key="default_fs_asset_store", asset_metadata={"path": "rawdata"}
        )
    ],
)
def call_api(_):
    return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


@solid(
    output_defs=[
        OutputDefinition(
            asset_store_key="default_fs_asset_store", asset_metadata={"path": "parse_df"}
        )
    ],
)
def parse_df(context, df):
    context.log.info(struct_to_string(df))
    result_df = df[:5]
    return result_df


@solid(
    output_defs=[
        OutputDefinition(
            asset_store_key="default_fs_asset_store", asset_metadata={"path": "model_result"},
        )
    ],
)
def train_model(context, df):
    context.log.info(struct_to_string(df))
    model = train(df)
    return model


@pipeline(
    mode_defs=[
        ModeDefinition("local", resource_defs={"default_fs_asset_store": local_asset_store})
    ],
    preset_defs=[
        PresetDefinition("local", run_config={"storage": {"filesystem": {}}}, mode="local")
    ],
)
def model_pipeline():
    train_model(parse_df(call_api()))


if __name__ == "__main__":
    instance = DagsterInstance.ephemeral(address_store=AddressStore())

    result = execute_pipeline(model_pipeline, preset="local", instance=instance)

    # print("---reexecution 1 ⬇️---")

    re1_result = reexecute_pipeline(
        model_pipeline,
        result.run_id,
        preset="local",
        instance=instance,
        step_selection=["parse_df.compute"],
    )
    # print("---reexecution 2 ⬇️---")

    re2_result = reexecute_pipeline(
        model_pipeline,
        re1_result.run_id,
        preset="local",
        instance=instance,
        step_selection=["parse_df.compute"],
    )
