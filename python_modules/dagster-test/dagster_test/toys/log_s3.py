from dagster import (
    AssetKey,
    AssetMaterialization,
    EventMetadataEntry,
    Field,
    Output,
    pipeline,
    solid,
)


@solid(
    config_schema={"bucket": Field(str, is_required=True), "key": Field(str, is_required=True),}
)
def read_key(context):
    key = context.solid_config["key"]
    bucket = context.solid_config["bucket"]
    path = f"s3://{bucket}/{key}"
    context.log.info(f"Found file {path}")
    yield AssetMaterialization(
        asset_key=AssetKey(["log_s3", path]),
        metadata_entries=[EventMetadataEntry.url(path, "S3 path")],
    )
    yield Output(path)


@pipeline(description="Demo pipeline that spits out some file info, given a path")
def log_s3_pipeline():
    read_key()
