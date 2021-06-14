from typing import Dict, NamedTuple, Optional, Any

from dagster import check
from dagster.config import ConfigType, Shape, Field

from .config import ConfigMapping, ConfigChanges
from .pipeline import PipelineDefinition


class RunConfigSchema(NamedTuple):
    run_config_schema_type: ConfigType
    config_type_dict_by_name: Dict[str, ConfigType]
    config_type_dict_by_key: Dict[str, ConfigType]
    config_changes: Optional[ConfigChanges]

    def has_config_type(self, name):
        check.str_param(name, "name")
        return name in self.config_type_dict_by_name

    def config_type_named(self, name):
        check.str_param(name, "name")
        return self.config_type_dict_by_name[name]

    def config_type_keyed(self, key):
        check.str_param(key, "key")
        return self.config_type_dict_by_key[key]

    def all_config_types(self):
        return self.config_type_dict_by_key.values()

    @property
    def config_mapping(self) -> Optional[ConfigMapping]:
        if self.config_changes and self.config_changes.config_mapping:
            return self.config_changes.config_mapping

        return None

    @property
    def config_type(self):
        if self.config_changes:
            return self.config_changes.apply_over(self.run_config_schema_type)


def create_run_config_schema(pipeline_def, mode=None):
    check.inst_param(pipeline_def, "pipeline_def", PipelineDefinition)
    mode = check.opt_str_param(mode, "mode", default=pipeline_def.get_default_mode_name())

    return pipeline_def.get_run_config_schema(mode)


def create_run_config_schema_type(pipeline_def, mode=None):
    return create_run_config_schema(pipeline_def, mode).config_type
