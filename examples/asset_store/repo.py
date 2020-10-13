import os
from dagster import (
    ModeDefinition,
    pipeline,
    solid,
    Output,
    resource,
    execute_pipeline,
    reexecute_pipeline,
    DagsterInstance,
)
from dagster.core.definitions.preset import PresetDefinition
from dagster.core.definitions.utils import struct_to_string

# from dagster.core.storage.address_store import (
#     PickledObjectFileystemAssetStore,
# )


def train(df):
    return len(df)


@solid
def call_api(_):
    df = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    return Output(value=df, address="rawdata")


@solid
def parse_df(context, df):
    context.log.info(struct_to_string(df))

    result_df = df[:5]
    return Output(
        value=result_df,
        address="parse_df",
        # object_store=context.resources.default_asset_store,
    )


@solid
def train_model(context, df):
    context.log.info(struct_to_string(df))

    model = train(df)
    return Output(
        value=model,
        address="model_result",
        # object_store=context.resources.default_asset_store,
    )


# @pipeline(
#     mode_defs=[
#         ModeDefinition(
#             "foo", resource_defs={"default_asset_store": PickledObjectFileystemAssetStore},
#         )
#     ],
# )
@pipeline(
    preset_defs=[
        PresetDefinition(
            "local",
            {
                "intermediate_storage": {
                    "address_store": {"config": {"base_dir": "uncommitted/intermediates"}}
                },
                "storage": {"filesystem": {}},
            },
        )
    ]
)
def model_pipeline():
    train_model(parse_df(call_api()))


if __name__ == "__main__":
    instance = DagsterInstance.ephemeral()

    result = execute_pipeline(model_pipeline, preset="local")

    re1_result = reexecute_pipeline(
        model_pipeline,
        result.run_id,
        preset="local",
        instance=instance,
        step_selection=["parse_df.compute"],
    )
    re2_result = reexecute_pipeline(
        model_pipeline,
        re1_result.run_id,
        preset="local",
        instance=instance,
        step_selection=["parse_df.compute"],
    )
