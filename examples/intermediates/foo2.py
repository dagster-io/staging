import pandas as pd
from dagster import ModeDefinition, pipeline, solid, Output, resource


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


@solid(required_resource_keys={"api", "s3_con"})
def call_api(context, request):
    df = context.resources.api.call(request)
    return Output(
        value=df,
        store=store,
        load=load,
        config={"format": "parquet", "path": "s3://some.domain/bucket/rawdata"},
    )


@solid(required_resource_keys={"mysql_con"})
def parse_df(context, df):
    result_df = parse(df)
    """
    - write/read pair: `store`, `load`
    - address: `config` - format, path, and more write/read options
    - storage: coupled with write/read pair
    """
    return Output(
        value=result_df, store=store, load=load, config={"format": "sql", "path": "feature_table"},
    )


@solid(required_resource_keys={"s3_con"})
def train_model(context, df):
    model = train(df)
    return Output(
        value=model,
        store=store,
        load=load,
        config={"format": "pickle", "path": "s3://some.domain/bucket/model_result"},
    )


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={
                "api": slack_resource,
                "mysql_con": mysql_resource,
                "s3_con": s3_resource,
            }
        )
    ]
)
def model():
    train_model(parse_df(call_api()))
