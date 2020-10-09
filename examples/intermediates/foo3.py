import pandas as pd
from dagster import (
    ModeDefinition,
    pipeline,
    solid,
    Output,
    resource,
    PresetDefinition,
    OutputDefinition,
)


@resource
def slack_resource(_):
    pass


@resource
def mysql_resource(_):
    pass


@resource
def s3_resource(_):
    pass


def parse(df):
    return


def train(df):
    return


@materializer(config_schema={"format": str, "path": str})
def store(context, value, config):
    if config["format"] == "parquet":
        value.to_parquet(config["path"], context.resources.s3_con)
    if config["format"] == "sql":
        value.to_sql(config["path"], context.resources.mysql_con)
    if config["format"] == "pickle":
        value.to_pickle(config["path"], context.resources.s3_con)


@loader(config_schema={"format": str, "path": str})
def load(context, config):
    if config["format"] == "parquet":
        pd.read_parquet(config["path"], context.resources.s3_con)
    if config["format"] == "sql":
        pd.read_sql(f"SELECT * FROM {config['path']}", context.resources.s3_con)
    if config["format"] == "pickle":
        pd.read_pickle(config["path"], context.resources.s3_con)


@solid(
    required_resource_keys={"slack", "s3_con"},
    output_defs=[OutputDefinition(store=store, load=load)],
)
def call_api(context, request):
    df = context.resources.slack.call_api(request)
    return Output(value=df)


@solid(required_resource_keys={"mysql_con"}, output_defs=[OutputDefinition(store=store, load=load)])
def parse_df(context, df):
    result_df = parse(df)
    """
    - write/read pair: `store`, `load`
    - address: `config` provided via run_config - format, path, and more write/read options
    - storage: configured at pipeline level
    """
    return Output(value=result_df)


@solid(required_resource_keys={"s3_con"}, output_defs=[OutputDefinition(store=store, load=load)])
def train_model(context, df):
    model = train(df)
    return Output(value=model)


@pipeline(
    mode_defs=[
        ModeDefinition(
            "foo",
            resource_defs={
                "slack": slack_resource,
                "mysql_con": mysql_resource,
                "s3_con": s3_resource,
            },
        )
    ],
    preset_defs=[
        PresetDefinition(
            "foo",
            {
                "solids": {
                    "call_api": {
                        "inputs": {"request": {"send_message"}},
                        "outputs": [
                            {
                                "result": {
                                    "format": "parquet",
                                    "path": "s3://some.domain/bucket/rawdata",
                                }
                            }
                        ],
                    },
                    "parse_df": {
                        "outputs": [{"result": {"format": "sql", "path": "feature_table"}}],
                    },
                    "train_model": {
                        "outputs": [
                            {
                                "result": {
                                    "format": "pickle",
                                    "path": "s3://some.domain/bucket/model_result",
                                }
                            }
                        ],
                    },
                },
                "storage": {"filesystem": {}},
            },
        )
    ],
)
def model():
    train_model(parse_df(call_api()))
