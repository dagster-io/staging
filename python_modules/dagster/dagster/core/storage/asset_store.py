import os
import pickle
from abc import abstractmethod
from collections import namedtuple
from functools import update_wrapper

from dagster import check
from dagster.config import Field
from dagster.config.field_utils import check_user_facing_opt_config_param
from dagster.config.source import StringSource
from dagster.core.definitions.config import is_callable_valid_config_arg
from dagster.core.definitions.events import AssetKey, EventMetadataEntry
from dagster.core.definitions.resource import resource
from dagster.core.events import AssetMaterialization
from dagster.core.execution.context.system import AssetStoreContext
from dagster.core.storage.input_manager import InputManager, InputManagerDefinition
from dagster.serdes import whitelist_for_serdes
from dagster.utils import PICKLE_PROTOCOL, mkdir_p
from dagster.utils.backcompat import experimental


@whitelist_for_serdes
class AssetStoreHandle(namedtuple("_AssetStoreHandle", "asset_store_key asset_metadata")):
    def __new__(cls, asset_store_key, asset_metadata=None):
        return super(AssetStoreHandle, cls).__new__(
            cls,
            asset_store_key=check.str_param(asset_store_key, "asset_store_key"),
            asset_metadata=check.opt_dict_param(asset_metadata, "asset_metadata", key_type=str),
        )


class AssetStoreDefinition(InputManagerDefinition):
    def __init__(
        self,
        resource_fn=None,
        config_schema=None,
        description=None,
        _configured_config_mapping_fn=None,
        version=None,
        input_config_schema=None,
        output_config_schema=None,
    ):
        self._output_config_schema = output_config_schema
        super(AssetStoreDefinition, self).__init__(
            resource_fn=resource_fn,
            config_schema=config_schema,
            description=description,
            _configured_config_mapping_fn=_configured_config_mapping_fn,
            version=version,
            input_config_schema=input_config_schema,
        )

    @property
    def output_config_schema(self):
        return self._output_config_schema


class AssetStore(InputManager):
    """
    Base class for user-provided asset store.

    Extend this class to handle asset operations. Users should implement ``set_asset`` to store a
    data object that can be tracked by the Dagster machinery and ``get_asset`` to retrieve a data
    object.
    """

    @abstractmethod
    def set_asset(self, context, obj):
        """The user-definied write method that stores a data object.

        Args:
            context (AssetStoreContext): The context of the step output that produces this asset.
            obj (Any): The data object to be stored.
        """


def asset_store(
    config_schema=None,
    description=None,
    version=None,
    output_config_schema=None,
    input_config_schema=None,
):
    if callable(config_schema) and not is_callable_valid_config_arg(config_schema):
        return _AssetStoreDecoratorCallable()(config_schema)

    def _wrap(resource_fn):
        return _AssetStoreDecoratorCallable(
            config_schema=config_schema,
            description=description,
            version=version,
            output_config_schema=output_config_schema,
            input_config_schema=input_config_schema,
        )(resource_fn)

    return _wrap


class _AssetStoreDecoratorCallable:
    def __init__(
        self,
        config_schema=None,
        description=None,
        version=None,
        output_config_schema=None,
        input_config_schema=None,
    ):
        self.config_schema = check_user_facing_opt_config_param(config_schema, "config_schema")
        self.description = check.opt_str_param(description, "description")
        self.version = check.opt_str_param(version, "version")
        self.output_config_schema = check_user_facing_opt_config_param(
            output_config_schema, "output_config_schema"
        )
        self.input_config_schema = check_user_facing_opt_config_param(
            input_config_schema, "input_config_schema"
        )

    def __call__(self, fn):
        check.callable_param(fn, "fn")

        asset_store_def = AssetStoreDefinition(
            resource_fn=fn,
            config_schema=self.config_schema,
            description=self.description,
            version=self.version,
            output_config_schema=self.output_config_schema,
            input_config_schema=self.input_config_schema,
        )

        update_wrapper(asset_store_def, wrapped=fn)

        return asset_store_def


class InMemoryAssetStore(AssetStore):
    def __init__(self):
        self.values = {}

    def set_asset(self, context, obj):
        keys = tuple(context.get_run_scoped_output_identifier())
        self.values[keys] = obj

    def get_asset(self, context):
        keys = tuple(context.get_run_scoped_output_identifier())
        return self.values[keys]


@asset_store
def mem_asset_store(_):
    return InMemoryAssetStore()


class PickledObjectFilesystemAssetStore(AssetStore):
    """Built-in filesystem asset store that stores and retrieves values using pickling.

    Args:
        base_dir (Optional[str]): base directory where all the step outputs which use this asset
            store will be stored in.
    """

    def __init__(self, base_dir=None):
        self.base_dir = check.opt_str_param(base_dir, "base_dir")
        self.write_mode = "wb"
        self.read_mode = "rb"

    def _get_path(self, context):
        """Automatically construct filepath."""
        keys = context.get_run_scoped_output_identifier()

        return os.path.join(self.base_dir, *keys)

    def set_asset(self, context, obj):
        """Pickle the data and store the object to a file.

        This method omits the AssetMaterialization event so assets generated by it won't be tracked
        by the Asset Catalog.
        """
        check.inst_param(context, "context", AssetStoreContext)

        filepath = self._get_path(context)

        # Ensure path exists
        mkdir_p(os.path.dirname(filepath))

        with open(filepath, self.write_mode) as write_obj:
            pickle.dump(obj, write_obj, PICKLE_PROTOCOL)

    def get_asset(self, context):
        """Unpickle the file and Load it to a data object."""
        check.inst_param(context, "context", AssetStoreContext)

        filepath = self._get_path(context)

        with open(filepath, self.read_mode) as read_obj:
            return pickle.load(read_obj)


@resource(config_schema={"base_dir": Field(StringSource, default_value=".", is_required=False)})
@experimental
def fs_asset_store(init_context):
    """Built-in filesystem asset store that stores and retrieves values using pickling.

    It allows users to specify a base directory where all the step output will be stored in. It
    serializes and deserializes output values (assets) using pickling and automatically constructs
    the filepaths for the assets.

    Example usage:

    1. Specify a pipeline-level asset store using the reserved resource key ``"asset_store"``,
    which will set the given asset store on all solids across a pipeline.

    .. code-block:: python

        @solid
        def solid_a(context, df):
            return df

        @solid
        def solid_b(context, df):
            return df[:5]

        @pipeline(mode_defs=[ModeDefinition(resource_defs={"asset_store": fs_asset_store})])
        def pipe():
            solid_b(solid_a())


    2. Specify asset store on :py:class:`OutputDefinition`, which allows the user to set different
    asset stores on different step outputs.

    .. code-block:: python

        @solid(output_defs=[OutputDefinition(asset_store_key="my_asset_store")])
        def solid_a(context, df):
            return df

        @solid
        def solid_b(context, df):
            return df[:5]

        @pipeline(
            mode_defs=[ModeDefinition(resource_defs={"my_asset_store": fs_asset_store})]
        )
        def pipe():
            solid_b(solid_a())

    """

    return PickledObjectFilesystemAssetStore(init_context.resource_config["base_dir"])


class CustomPathPickledObjectFilesystemAssetStore(AssetStore):
    """Built-in filesystem asset store that stores and retrieves values using pickling and
    allow users to specify file path for outputs.

    Args:
        base_dir (Optional[str]): base directory where all the step outputs which use this asset
            store will be stored in.
    """

    def __init__(self, base_dir=None):
        self.base_dir = check.opt_str_param(base_dir, "base_dir")
        self.write_mode = "wb"
        self.read_mode = "rb"

    def _get_path(self, path):
        return os.path.join(self.base_dir, path)

    def set_asset(self, context, obj):
        """Pickle the data and store the object to a custom file path.

        This method emits an AssetMaterialization event so the assets will be tracked by the
        Asset Catalog.
        """
        check.inst_param(context, "context", AssetStoreContext)
        asset_metadata = context.asset_metadata
        path = check.str_param(asset_metadata.get("path"), "asset_metadata.path")

        filepath = self._get_path(path)

        # Ensure path exists
        mkdir_p(os.path.dirname(filepath))

        with open(filepath, self.write_mode) as write_obj:
            pickle.dump(obj, write_obj, PICKLE_PROTOCOL)

        return AssetMaterialization(
            asset_key=AssetKey([context.pipeline_name, context.step_key, context.output_name]),
            metadata_entries=[EventMetadataEntry.fspath(os.path.abspath(filepath))],
        )

    def get_asset(self, context):
        """Unpickle the file from a given file path and Load it to a data object."""
        asset_metadata = context.asset_metadata
        path = check.str_param(asset_metadata.get("path"), "asset_metadata.path")
        filepath = self._get_path(path)

        with open(filepath, self.read_mode) as read_obj:
            return pickle.load(read_obj)


@resource(config_schema={"base_dir": Field(StringSource, default_value=".", is_required=False)})
@experimental
def custom_path_fs_asset_store(init_context):
    """Built-in asset store that allows users to custom output file path per output definition.

    It also allows users to specify a base directory where all the step output will be stored in. It
    serializes and deserializes output values (assets) using pickling and stores the pickled object
    in the user-provided file paths.

    Example usage:

    .. code-block:: python

        @solid(
            output_defs=[
                OutputDefinition(
                    asset_store_key="asset_store", asset_metadata={"path": "path/to/sample_output"}
                )
            ]
        )
        def sample_data(context, df):
            return df[:5]

        @pipeline(
            mode_defs=[
                ModeDefinition(resource_defs={"asset_store": custom_path_fs_asset_store}),
            ],
        )
        def pipe():
            sample_data()
    """
    return CustomPathPickledObjectFilesystemAssetStore(init_context.resource_config["base_dir"])
