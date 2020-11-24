import logging
import pickle

from dagster import (
    AssetKey,
    AssetMaterialization,
    AssetStore,
    EventMetadataEntry,
    Field,
    Selector,
    StringSource,
    check,
    resource,
)
from dagster.utils import PICKLE_PROTOCOL
from dagster_azure.adls2.resources import create_adls2_client, create_blob_client
from dagster_azure.adls2.utils import ResourceNotFoundError

_LEASE_DURATION = 60  # One minute


class PickledObjectADLS2AssetStore(AssetStore):
    def __init__(self, file_system, adls2_client, blob_client, prefix="dagster"):
        self.adls2_client = adls2_client
        self.file_system_client = self.adls2_client.get_file_system_client(file_system)
        # We also need a blob client to handle copying as ADLS doesn't have a copy API yet
        self.blob_client = blob_client
        self.blob_container_client = self.blob_client.get_container_client(file_system)
        self.prefix = check.str_param(prefix, "prefix")

        self.lease_duration = _LEASE_DURATION
        self.file_system_client.get_file_system_properties()

    def _get_path(self, context):
        return "/".join(
            [
                self.prefix,
                "storage",
                context.source_run_id,
                "files",
                context.step_key,
                context.output_name,
            ]
        )

    def _last_key(self, key):
        if "/" not in key:
            return key
        comps = key.split("/")
        return comps[-1]

    def _rm_object(self, key):
        check.str_param(key, "key")
        check.param_invariant(len(key) > 0, "key")

        # This operates recursively already so is nice and simple.
        self.file_system_client.delete_file(key)

        return self._uri_for_key(key)

    def _has_object(self, key):
        check.str_param(key, "key")
        check.param_invariant(len(key) > 0, "key")

        try:
            file = self.file_system_client.get_file_client(key)
            file.get_file_properties()
            return True
        except ResourceNotFoundError:
            return False

    def _uri_for_key(self, key, protocol=None):
        check.str_param(key, "key")
        protocol = check.opt_str_param(protocol, "protocol", default="abfss://")
        return "{protocol}{filesystem}@{account}.dfs.core.windows.net/{key}".format(
            protocol=protocol,
            filesystem=self.file_system_client.file_system_name,
            account=self.file_system_client.account_name,
            key=key,
        )

    def get_asset(self, context):
        key = self._get_path(context)
        file = self.file_system_client.get_file_client(key)
        stream = file.download_file()
        obj = pickle.loads(stream.readall())

        return obj

    def set_asset(self, context, obj):
        key = self._get_path(context)
        logging.info("Writing ADLS2 object at: " + self._uri_for_key(key))

        if self._has_object(key):
            logging.warning("Removing existing ADLS2 key: {key}".format(key=key))
            self._rm_object(key)

        pickled_obj = pickle.dumps(obj, PICKLE_PROTOCOL)

        file = self.file_system_client.create_file(key)
        with file.acquire_lease(self.lease_duration) as lease:
            file.upload_data(pickled_obj, lease=lease, overwrite=True)

        return AssetMaterialization(
            asset_key=AssetKey([context.pipeline_name, context.step_key, context.output_name]),
            metadata_entries=[
                EventMetadataEntry.path(self._uri_for_key(key), label=self._last_key(key))
            ],
        )


@resource(
    config_schema={
        "adls2_file_system": Field(StringSource, description="ADLS Gen2 file system name"),
        "adls2_prefix": Field(StringSource, is_required=False, default_value="dagster"),
        "storage_account": Field(StringSource, description="The storage account name."),
        "credential": Field(
            Selector(
                {
                    "sas": Field(StringSource, description="SAS token for the account."),
                    "key": Field(StringSource, description="Shared Access Key for the account"),
                }
            ),
            description="The credentials with which to authenticate.",
        ),
    },
)
def adls2_asset_store(init_context):
    """Persistent asset store using Azure Data Lake Storage Gen2 for storage.

    Suitable for assets storage for distributed executors, so long as
    each execution node has network connectivity and credentials for ADLS and
    the backing container.

    Attach this resource definition to a :py:class:`~dagster.ModeDefinition`
    in order to make it available to your pipeline:

    .. code-block:: python

        pipeline_def = PipelineDefinition(
            mode_defs=[
                ModeDefinition(
                    resource_defs={'adls2': adls2_asset_store, ...},
                ), ...
            ], ...
        )

    You may configure this storage as follows:

    .. code-block:: YAML

        resources:
            adls2:
            config:
                adls2_file_system: my-cool-file-system
                adls2_prefix: good/prefix-for-files-
    """
    # TODO: Add adls2 client resource once resource dependencies are enabled.
    adls2_client = create_adls2_client(
        init_context.resource_config["storage_account"], init_context.resource_config["credential"]
    )
    blob_client = create_blob_client(
        init_context.resource_config["storage_account"], init_context.resource_config["credential"]
    )
    asset_store = PickledObjectADLS2AssetStore(
        init_context.resource_config["adls2_file_system"],
        adls2_client,
        blob_client,
        init_context.resource_config.get("adls2_prefix"),
    )
    return asset_store
