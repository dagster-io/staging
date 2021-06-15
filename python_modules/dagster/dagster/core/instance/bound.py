from typing import TYPE_CHECKING, Dict

from dagster.core.definitions import PipelineDefinition, RunConfigSchema, create_run_config_schema

if TYPE_CHECKING:
    from . import DagsterInstance


class BoundPipeline:
    def __init__(self, pipeline_def: PipelineDefinition, instance: "DagsterInstance"):
        self._pipeline_def = pipeline_def
        self._instance = instance
        self._run_config_schema_cache: Dict[str, RunConfigSchema] = {}

    def get_run_config_schema(self, mode: str):
        if mode not in self._run_config_schema_cache:
            self._run_config_schema_cache[mode] = create_run_config_schema(
                self._instance,
                self._pipeline_def,
                mode,
            )

        return self._run_config_schema_cache[mode]

    @property
    def pipeline_def(self):
        return self._pipeline_def

    def get_parent(self):
        return BoundPipeline(self._pipeline_def.parent_pipeline_def, self._instance)
