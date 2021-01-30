from typing import Any, Dict, List

from dagster import AssetMaterialization, Partition, PartitionSetDefinition, pipeline, solid


@solid
def unreliable_start(_):
    return 1


@solid(config_schema={"partition": str}, output_defs=[])
def emit_materialization(context):
    yield AssetMaterialization(
        asset_key=["partitioned_asset_pipeline_asset_key"],
        partition=context.solid_config["partition"],
    )


@pipeline(description="Emits partitioned asset materializations")
def partitioned_asset_pipeline():
    emit_materialization()


def get_partitions() -> List[Partition]:
    return [Partition(f"2020-05-{str(day).zfill(2)}") for day in range(1, 32)]


def get_run_config_for_partition(partition: Partition) -> Dict[str, Any]:
    return {"solids": {"emit_materialization": {"config": {"partition": partition.value}}}}


partitioned_asset_partition_set = PartitionSetDefinition(
    "partitioned_asset_partition_set",
    "partitioned_asset_pipeline",
    partition_fn=get_partitions,
    run_config_fn_for_partition=get_run_config_for_partition,
)
