from collections import namedtuple

from dagster import check
from dagster.config.config_type import ConfigType

from .executor import ExecutorDefinition
from .pipeline import PipelineDefinition


class RunConfigSchema(
    namedtuple(
        "_RunConfigSchema", "environment_type config_type_dict_by_name config_type_dict_by_key"
    )
):
    def __new__(cls, environment_type, config_type_dict_by_name, config_type_dict_by_key):
        return super(RunConfigSchema, cls).__new__(
            cls,
            environment_type=check.inst_param(environment_type, "environment_type", ConfigType),
            config_type_dict_by_name=check.dict_param(
                config_type_dict_by_name,
                "config_type_dict_by_name",
                key_type=str,
                value_type=ConfigType,
            ),
            config_type_dict_by_key=check.dict_param(
                config_type_dict_by_key,
                "config_type_dict_by_key",
                key_type=str,
                value_type=ConfigType,
            ),
        )

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


def create_run_config_schema(pipeline_def, default_executor_defs, mode=None):
    check.inst_param(pipeline_def, "pipeline_def", PipelineDefinition)
    check.list_param(default_executor_defs, "default_executor_defs", of_type=ExecutorDefinition)
    mode = check.opt_str_param(mode, "mode", default=pipeline_def.get_default_mode_name())

    return pipeline_def.get_run_config_schema(default_executor_defs, mode)


def create_environment_type(pipeline_def, default_executor_defs, mode=None):
    return create_run_config_schema(pipeline_def, default_executor_defs, mode).environment_type
