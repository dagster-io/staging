import os
from dagster import (
    ModeDefinition,
    pipeline,
    solid,
    Output,
    resource,
)


@resource
def slack_resource(_):
    pass


def parse(df):
    return


def train(df):
    return


class AddressableAssetEvent:
    """
    Event related addressableAssets
    """

    def __init__(self, address, context_data):
        self.address = address
        self.context_data = context_data


class AssetAddress:
    """
    Pointer to an addressable asset.
    it contains the metadata of an addressable assets
    """


class AssetStore:
    """
    - handle write and read user-defined function
    - create AssetAddress for addressale asset tracking
    """


class AddressStore:
    """
    Instance-level mapping: step_output_handle -> address
    can be restored cross runs using AddressableAssetEvent
    """

    def __init__(self, mapping=None):
        self.mapping = mapping or {}

    def set_address(self, context, address):
        self.mapping[context.step_output_handle] = address
        return AddressableAssetEvent(address, context.step_output_handle)

    def get_address(self, context):
        return self.mapping[context.step_output_handle]


class ParquetObjectS3AssetAddress(AssetAddress):
    def __init__(self, asset_id, path):
        self.asset_id = asset_id
        self.path = path


class ParquetObjectS3AssetStore(AssetStore):
    def __init__(self, base_dir):
        self.base_dir = base_dir
        # and all sorts of s3 config

    def store_to_file(self, obj, path):
        # user defined code
        pass

    def load_from_file(self, path):
        # user defined code
        pass

    def _get_path(self, path):
        return os.path.join(self.base_dir, path)

    def set_asset(self, _context, obj, path):
        """
        store data object to file and track it as AddressablAsset
        """
        asset_id = new_asset_id()

        address_path = self._get_path(path)
        self.store_to_file(obj, address_path)

        return ParquetObjectS3AssetAddress(asset_id, address_path)

    def get_asset(self, _context, address):
        """
        load data object from file using AssetAddress
        """
        # user defined code
        return self.load_from_file(address.path)


@solid(required_resource_keys={"slack", "default_asset_store"})
def call_api(context, request):
    df = context.resources.slack.call_api(request)
    return Output(
        value=df,
        address="s3://some.domain/bucket/rawdata",
        object_store=context.resources.default_asset_store,
    )


@solid(required_resource_keys={"default_asset_store"})
def parse_df(context, df):
    result_df = parse(df)

    return Output(
        value=result_df,
        address="s3://some.domain/bucket/parse_df",
        object_store=context.resources.default_asset_store,
    )


@solid(required_resource_keys={"default_asset_store"})
def train_model(context, df):
    model = train(df)
    return Output(
        value=model,
        address="s3://some.domain/bucket/model_result",
        object_store=context.resources.default_asset_store,
    )


@pipeline(
    mode_defs=[
        ModeDefinition(
            "foo",
            resource_defs={
                "slack": slack_resource,
                "default_asset_store": ParquetObjectS3AssetStore,
            },
        )
    ],
)
def model():
    train_model(parse_df(call_api()))
