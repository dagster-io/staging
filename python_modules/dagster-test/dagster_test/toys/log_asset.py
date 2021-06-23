from typing import List

from dagster import Array, graph, op


@op(config_schema={"asset_key": Array(str), "pipeline": str})
def read_materialization(context) -> List[str]:
    asset_key = context.solid_config["asset_key"]
    from_pipeline = context.solid_config["pipeline"]
    context.log.info(f"Found materialization for asset key {asset_key} in {from_pipeline}")
    return asset_key


@graph(description="Demo graph that logs asset materializations from other jobs")
def log_asset_graph():
    read_materialization()


log_asset_job = log_asset_graph.to_job(name="log_asset_job")
