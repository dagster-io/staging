# pylint: disable=unused-argument

from dagster import AssetKey, AssetMaterialization, EventMetadataEntry, Output, solid


def read_df():
    return 1


def persist_to_storage(df):
    pass


def calculate_bytes(df):
    return 1.0


# start_materialization_solids_marker_0
@solid
def my_simple_solid(_):
    df = read_df()
    persist_to_storage(df)
    return "done"


# end_materialization_solids_marker_0

# start_materialization_solids_marker_1
@solid
def my_materialization_solid(context):
    df = read_df()
    persist_to_storage(df)
    yield AssetMaterialization(asset_key="my_dataset", description="Persisted result to storage")
    yield Output("done")


# end_materialization_solids_marker_1


# start_materialization_solids_marker_2
@solid
def my_metadata_materialization_solid(context):
    df = read_df()
    persist_to_storage(df)
    yield AssetMaterialization(
        asset_key="my_dataset",
        description="Persisted result to storage",
        metadata_entries=[
            EventMetadataEntry.text("Text-based metadata for this event", label="text_metadata"),
            EventMetadataEntry.fspath("/path/to/data/on/filesystem"),
            EventMetadataEntry.url("http://mycoolsite.com/url_for_my_data", label="dashboard_url"),
            EventMetadataEntry.float(calculate_bytes(df), "size (bytes)"),
        ],
    )
    yield Output("done")


# end_materialization_solids_marker_2


# start_materialization_solids_marker_3
@solid
def my_asset_key_materialization_solid(context):
    df = read_df()
    persist_to_storage(df)
    yield AssetMaterialization(
        asset_key=AssetKey(["dashboard", "my_cool_site"]),
        description="Persisted result to storage",
        metadata_entries=[
            EventMetadataEntry.url("http://mycoolsite.com/dashboard", label="dashboard_url"),
            EventMetadataEntry.float(calculate_bytes(df), "size (bytes)"),
        ],
    )
    yield Output("done")


# end_materialization_solids_marker_3
