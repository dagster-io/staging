import hashlib
import textwrap
from typing import Any, Dict, List

from dagster import (
    AssetMaterialization,
    EventMetadataEntry,
    Partition,
    PartitionSetDefinition,
    pipeline,
    solid,
)


@solid
def unreliable_start(_):
    return 1


@solid(config_schema={"partition": str}, output_defs=[])
def emit_materialization(context):
    partition = context.solid_config["partition"]
    num_bad_records = int(hashlib.md5(partition.encode("utf-8")).hexdigest(), 16) % 500
    num_events_long_description = (
        "The number of events for the user. Note that in situation Z, this can severely undercount "
        "the number of events, due to the influence of factor L. Users are advised to take "
        "factor L into account when conducting analyses, especially when using the number of "
        "to answer questions like R."
    )

    yield AssetMaterialization(
        asset_key=["partitioned_asset_pipeline_asset_key"],
        description=textwrap.dedent(
            """
            The partitioned asset pipeline asset is an asset that users can use for X, Y, and Z.

            When using it for Y, they should be aware that using it for Y could cause problem P. The
            most important columns in this asset are A, B, and C.
        """
        ).strip(),
        partition=partition,
        metadata_entries=[
            EventMetadataEntry.md(
                textwrap.dedent(
                    f"""
                    | Column | Type | Description |
                    | - | - | - |
                    | user_id | varchar | The ID of the user. |
                    | num_events | int | {num_events_long_description} |
                    """
                ).strip(),
                "Columns",
            ),
            EventMetadataEntry.int(num_bad_records, "Number of bad records"),
        ],
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
