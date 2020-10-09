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


@solid(required_resource_keys={"api", "s3_con"})
def call_api(context, request):
    df = context.resources.api.call(request)
    return Output(
        value=df,
        store=lambda value, path: value.to_parquet(path, context.resources.s3_con),
        load=lambda path: pd.read_parquet(path, context.resources.s3_con),
        path="s3://some.domain/bucket/rawdata",
    )


@solid(required_resource_keys={"mysql_con"})
def parse_df(context, df):
    result_df = parse(df)
    """
    - write/read pair: `store`, `load`
    - address: `path`
    - storage: `context.resources.some_resource`
    """
    return Output(
        value=result_df,
        store=lambda value, path: value.to_sql(f"{path}", context.resources.mysql_con),
        load=lambda path: pd.read_sql(f"SELECT * FROM {path}", context.resources.mysql_con),
        path="feature_table",
    )


@solid(required_resource_keys={"s3_con"})
def train_model(context, df):
    model = train(df)
    return Output(
        value=model,
        store=lambda value, path: value.to_pickle(path, context.resources.s3_con),
        load=lambda path: pd.read_pickle(path, context.resources.s3_con),
        path="s3://some.domain/bucket/model_result",
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
