from dagster import Field, pipeline, solid


@solid(config_schema={"path": Field(str, is_required=True), "bucket": Field(str, is_required=True)})
def log_path(context):
    context.log.info(
        "Found path {path} in bucket {bucket}.".format(
            path=context.solid_config.get("path"), bucket=context.solid_config.get("bucket"),
        )
    )


@pipeline
def log_s3_pipeline():
    log_path()
