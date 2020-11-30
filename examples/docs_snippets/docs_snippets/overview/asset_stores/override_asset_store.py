# pylint: disable=unused-argument


from dagster import (
    AssetStore,
    InputDefinition,
    ModeDefinition,
    OutputDefinition,
    pipeline,
    resource,
    solid,
)
from dagster.core.storage.input_manager import input_manager


def read_spark_from_table(**_kwargs):
    return 1


def write_pandas_to_table(**_kwargs):
    pass


def read_pandas_from_table(**_kwargs):
    raise NotImplementedError()


class MyAssetStore(AssetStore):
    def set_asset(self, context, obj):
        table_name = context.output_name
        write_pandas_to_table(name=table_name, dataframe=obj)

    def get_asset(self, context):
        table_name = context.output_name
        return read_pandas_from_table(name=table_name)


@resource
def my_asset_store(_):
    return MyAssetStore()


@input_manager
def spark_table_loader(context, _resource_config, _input_config):
    return read_spark_from_table(name=context.output_name)


@solid(output_defs=[OutputDefinition(manager_key="my_asset_store")])
def solid1(_):
    """Return a Pandas DataFrame"""


@solid(input_defs=[InputDefinition("input1", manager_key="spark_loader")])
def solid2(_, input1):
    """Return a Spark DataFrame"""


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={"my_asset_store": my_asset_store, "spark_loader": spark_table_loader}
        )
    ]
)
def my_pipeline():
    solid2(solid1())
