from dagster import DagsterInstance, Output, execute_pipeline, pipeline, reexecute_pipeline, solid
from dagster.core.definitions.preset import PresetDefinition
from dagster.core.definitions.utils import struct_to_string
from dagster.core.storage.asset_store import PickledObjectFileystemAssetStore


def train(df):
    return len(df)


default_asset_store = PickledObjectFileystemAssetStore("uncommitted/intermediates/")


# @solid(required_resource_keys={"default_asset_store"})
@solid
def call_api(_):
    df = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    return Output(value=df, address="rawdata", asset_store=default_asset_store)


# @solid(required_resource_keys={"default_asset_store"})
@solid
def parse_df(context, df):
    context.log.info(struct_to_string(df))

    result_df = df[:5]
    return Output(value=result_df, address="parse_df", asset_store=default_asset_store)


# @solid(required_resource_keys={"default_asset_store"})
@solid
def train_model(context, df):
    context.log.info(struct_to_string(df))

    model = train(df)
    return Output(value=model, address="model_result", asset_store=default_asset_store)


@pipeline(preset_defs=[PresetDefinition("local", {"storage": {"filesystem": {}}})])
def model_pipeline():
    train_model(parse_df(call_api()))


if __name__ == "__main__":
    instance = DagsterInstance.ephemeral()

    result = execute_pipeline(model_pipeline, preset="local", instance=instance)

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
