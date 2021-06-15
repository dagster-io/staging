from typing import Dict

from dagster import check
from dagster.config.snap import ConfigSchemaSnapshot, snap_from_config_type
from dagster.core.definitions.pipeline import PipelineDefinition
from dagster.core.definitions.run_config_schema import create_run_config_schema
from dagster.core.instance.bound import BoundPipeline
from dagster.core.snap import ConfigTypeSnap
from dagster.utils import merge_dicts


def build_config_schema_snapshot(bound_pipeline: BoundPipeline) -> ConfigSchemaSnapshot:

    all_config_snaps_by_key: Dict[str, ConfigTypeSnap] = {}
    for mode in bound_pipeline.pipeline_def.available_modes:
        run_config_schema = bound_pipeline.get_run_config_schema(mode)
        all_config_snaps_by_key = merge_dicts(
            all_config_snaps_by_key,
            {ct.key: snap_from_config_type(ct) for ct in run_config_schema.all_config_types()},
        )

    return ConfigSchemaSnapshot(all_config_snaps_by_key)
