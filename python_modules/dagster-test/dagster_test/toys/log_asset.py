from dagster import Array, AssetKey, Output, OutputDefinition, pipeline, solid


@solid(config_schema={"asset_key": Array(str), "pipeline": str})
def read_materialization(context):
    asset_key = context.solid_config["asset_key"]
    from_pipeline = context.solid_config["pipeline"]
    context.log.info(f"Found materialization for asset key {asset_key} in {from_pipeline}")
    yield Output(asset_key)


@solid(output_defs=[OutputDefinition(asset_key=AssetKey("my_table"))])
def write_table(_context, value):
    yield Output(value, metadata={})


@solid(output_defs=[OutputDefinition(asset_key=AssetKey("my_dashboard"))])
def write_dashboard(_context, value):
    yield Output(value, metadata={})


@pipeline(description="Demo pipeline that logs asset materializations from other pipelines")
def log_asset_pipeline():
    value = read_materialization()
    write_table(value)
    write_dashboard(value)
