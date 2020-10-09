import pandas as pd
from dagster import (
    ModeDefinition,
    pipeline,
    solid,
    Output,
    resource,
    PresetDefinition,
    OutputDefinition,
    SerializationStrategy,
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


class AssetAddress:
    pass


class AssetStorage:
    pass


class PickledObjectFilesystemAssetAddress(AssetAddress):
    def __init__(self, asset_id, path):
        self.asset_id = asset_id
        self.path = path
        # also include all python, picking version information


class PickledObjectFilesystemAssetStorage(AssetStorage):
    """
    PickledObjectFilesystemAssetStorage couples
    file format (csv, pickle, parquet, sql, etc) and storage (s3, snowflake, redshift, etc)

    [?] how does it handle the case where the users wants to
    1) store/load a python dict as a pickle to local filesystem (default)
    1) store/load a pyspark dataframe in parquet format to s3
    2) store/load a pyspark dataframe in parquet format to snowflake
    3) store/load a pandas dataframe in sql format to redshift
    """

    def __init__(self, base_dir):
        self.base_dir = base_dir

    def store_asset(self, _context, obj):
        asset_id = new_asset_id()
        # store to file
        return PickledObjectFilesystemAssetAddress(asset_id)

    def load_asset(self, _context, address):
        # load from file
        return get_asset(address)


class ParquetObjectS3AssetStorage(AssetStorage):
    """
    [?] how does it handle different data object type with the same storage and format
        different types of data object call different `to_file` functions
    """

    def __init__(self, base_dir):
        self.base_dir = base_dir

    def store_asset(self, _context, obj, path):
        asset_id = new_asset_id()
        # store to file
        if isinstance(obj, pyspark.DataFrame):
            obj.write_parquet(path)
        elif isinstance(obj, pandas.DataFrame):
            obj.to_parquet(path)

        # store_to_file(object, type, load_option)

        return ParquetObjectS3AssetAddress(asset_id)

    def load_asset(self, _context, address):
        # load from file
        if isinstance(obj, pyspark.DataFrame):
            return pyspark.read_parquet(address)
        elif isinstance(obj, pandas.DataFrame):
            return pandas.read_parquet(address)

        # load_from_file(object, type, load_option)


@solid(required_resource_keys={"slack"})
def call_api(context, request):
    df = context.resources.slack.call_api(request)
    return Output(value=df, address="s3://some.domain/bucket/rawdata")


@solid
def parse_df(context, df):
    result_df = parse(df)
    """
    - write/read pair: `store`, `load` configured at pipeline level in "default_asset_storage"
    - address: `config` provided via run_config - format, path, and more write/read options
    - storage: configured at pipeline level in "default_asset_storage"
    """
    return Output(value=result_df, address="s3://some.domain/bucket/parse_df")


@solid
def train_model(context, df):
    model = train(df)
    return Output(value=model, address="s3://some.domain/bucket/model_result")


@pipeline(
    mode_defs=[
        ModeDefinition(
            "foo",
            resource_defs={
                "slack": slack_resource,
                "s3_con": s3_resource,
                "default_asset_storage": ParquetObjectS3AssetStorage,
            },
        )
    ],
)
def model():
    train_model(parse_df(call_api()))
