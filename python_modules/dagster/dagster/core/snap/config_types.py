from dagster import check
from dagster.config.snap import ConfigSchemaSnapshot, snap_from_config_type
from dagster.core.definitions.executor import ExecutorDefinition
from dagster.core.definitions.pipeline import PipelineDefinition
from dagster.utils import merge_dicts


def build_config_schema_snapshot(pipeline_def, default_executor_defs):
    check.inst_param(pipeline_def, "pipeline_def", PipelineDefinition)
    check.list_param(default_executor_defs, "default_executor_defs", of_type=ExecutorDefinition)
    all_config_snaps_by_key = {}
    for mode in pipeline_def.available_modes:
        run_config_schema = pipeline_def.get_run_config_schema(default_executor_defs, mode)
        all_config_snaps_by_key = merge_dicts(
            all_config_snaps_by_key,
            {ct.key: snap_from_config_type(ct) for ct in run_config_schema.all_config_types()},
        )

    return ConfigSchemaSnapshot(all_config_snaps_by_key)
