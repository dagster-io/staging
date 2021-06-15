from typing import Dict, Iterable, NamedTuple, Optional, Set

from dagster import check
from dagster.config import ConfigType
from dagster.core.instance import DagsterInstance

from .config import ConfigChanges, ConfigMapping
from .mode import ModeDefinition
from .pipeline import PipelineDefinition
from .run_config import (
    RunConfigSchemaCreationData,
    construct_config_type_dictionary,
    define_run_config_schema_type,
)


class RunConfigSchema(NamedTuple):
    run_config_schema_type: ConfigType
    config_type_dict_by_name: Dict[str, ConfigType]
    config_type_dict_by_key: Dict[str, ConfigType]
    config_changes: Optional[ConfigChanges]

    def has_config_type(self, name: str) -> bool:
        return name in self.config_type_dict_by_name

    def config_type_named(self, name) -> ConfigType:
        return self.config_type_dict_by_name[name]

    def config_type_keyed(self, key: str):
        return self.config_type_dict_by_key[key]

    def all_config_types(self) -> Iterable[ConfigType]:
        return self.config_type_dict_by_key.values()

    @property
    def config_mapping(self) -> Optional[ConfigMapping]:
        if self.config_changes and self.config_changes.config_mapping:
            return self.config_changes.config_mapping

        return None

    @property
    def config_type(self) -> ConfigType:
        if self.config_changes:
            return self.config_changes.apply_over(self.run_config_schema_type)

        return self.run_config_schema_type


# def create_run_config_schema(
#     instance: DagsterInstance,
#     pipeline_def: PipelineDefinition,
#     mode: str = None,
# ):
#     return _create_run_config_schema(instance, pipeline_def, mode)


# probably drop
# def create_run_config_schema_type(pipeline_def, mode=None):
#     return create_run_config_schema(pipeline_def, mode).config_type


def create_run_config_schema(
    instance: DagsterInstance,
    pipeline_def: PipelineDefinition,
    mode: Optional[str],
    # mode_definition: ModeDefinition,
    # required_resources: Set[str],
) -> "RunConfigSchema":
    mode = mode or pipeline_def.get_default_mode_name()
    mode_definition = pipeline_def.get_mode_definition(mode)
    required_resources = set(pipeline_def.get_required_resource_defs_for_mode(mode).keys())

    # When executing with a subset pipeline, include the missing solids
    # from the original pipeline as ignored to allow execution with
    # run config that is valid for the original
    if pipeline_def.is_subset_pipeline:
        if pipeline_def.parent_pipeline_def is None:
            check.failed("Unexpected subset pipeline state")

        ignored_solids = [
            solid
            for solid in pipeline_def.parent_pipeline_def.solids
            if not pipeline_def.has_solid_named(solid.name)
        ]
    else:
        ignored_solids = []

    run_config_schema_type = define_run_config_schema_type(
        RunConfigSchemaCreationData(
            pipeline_name=pipeline_def.name,
            solids=pipeline_def.solids,
            dependency_structure=pipeline_def.dependency_structure,
            mode_definition=mode_definition,
            ignored_solids=ignored_solids,
            required_resources=required_resources,
            instance=instance,
        )
    )

    if mode_definition.config_changes:
        outer_config_type = mode_definition.config_changes.apply_over(run_config_schema_type)
    else:
        outer_config_type = run_config_schema_type

    if outer_config_type is None:
        check.failed("Unexpected outer_config_type value of None")

    config_type_dict_by_name, config_type_dict_by_key = construct_config_type_dictionary(
        pipeline_def.all_solid_defs,
        outer_config_type,
    )

    return RunConfigSchema(
        run_config_schema_type=run_config_schema_type,
        config_type_dict_by_name=config_type_dict_by_name,
        config_type_dict_by_key=config_type_dict_by_key,
        config_changes=mode_definition.config_changes,
    )
