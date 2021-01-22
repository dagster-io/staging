# Contains mode, resources, loggers
from collections import namedtuple

from dagster import check
from dagster.config.snap import ConfigFieldSnap, snap_from_field
from dagster.core.definitions import (
    IntermediateStorageDefinition,
    LoggerDefinition,
    ModeDefinition,
    ResourceDefinition,
)
from dagster.serdes import whitelist_for_serdes


def build_mode_def_snap(mode_def, root_config_key):
    check.inst_param(mode_def, "mode_def", ModeDefinition)
    check.str_param(root_config_key, "root_config_key")

    return ModeDefSnap(
        name=mode_def.name,
        description=mode_def.description,
        resource_def_snaps=sorted(
            [build_resource_def_snap(name, rd) for name, rd in mode_def.resource_defs.items()],
            key=lambda item: item.name,
        ),
        logger_def_snaps=sorted(
            [build_logger_def_snap(name, ld) for name, ld in mode_def.loggers.items()],
            key=lambda item: item.name,
        ),
        root_config_key=root_config_key,
        intermediate_storage_def_snaps=sorted(
            [
                build_intermediate_storage_def_snap(intermediate_storage_def)
                for intermediate_storage_def in mode_def.intermediate_storage_defs
            ],
            key=lambda item: item.name,
        ),
    )


@whitelist_for_serdes
class IntermediateStorageDefSnap(namedtuple("_IntermediateStorageDefSnap", "name is_persistent",)):
    def __new__(cls, name, is_persistent):
        return super(IntermediateStorageDefSnap, cls).__new__(
            cls,
            name=check.str_param(name, "name"),
            is_persistent=check.bool_param(is_persistent, "is_persistent"),
        )


def build_intermediate_storage_def_snap(intermediate_storage_def):
    check.inst_param(
        intermediate_storage_def, "intermediate_storage_def", IntermediateStorageDefinition
    )
    return IntermediateStorageDefSnap(
        name=intermediate_storage_def.name, is_persistent=intermediate_storage_def.is_persistent
    )


@whitelist_for_serdes
class ModeDefSnap(
    namedtuple(
        "_ModeDefSnap",
        "name description resource_def_snaps logger_def_snaps root_config_key intermediate_storage_def_snaps",
    )
):
    def __new__(
        cls,
        name,
        description,
        resource_def_snaps,  # Previously stored as a list without the resource key
        logger_def_snaps,
        root_config_key=None,
        intermediate_storage_def_snaps=None,
    ):
        return super(ModeDefSnap, cls).__new__(
            cls,
            name=check.str_param(name, "name"),
            description=check.opt_str_param(description, "description"),
            resource_def_snaps=check.opt_list_param(
                resource_def_snaps, "resource_def_snaps", of_type=ResourceDefSnap
            ),
            logger_def_snaps=check.list_param(
                logger_def_snaps, "logger_def_snaps", of_type=LoggerDefSnap
            ),
            root_config_key=check.opt_str_param(root_config_key, "root_config_key"),
            intermediate_storage_def_snaps=check.opt_list_param(
                intermediate_storage_def_snaps,
                "intermediate_storage_def_snaps",
                of_type=IntermediateStorageDefSnap,
            ),
        )

    def get_resource_def_snap(self, resource_name):
        resource_snaps_by_name = {
            resource_snap.name: resource_snap for resource_snap in self.resource_def_snaps
        }
        return resource_snaps_by_name[resource_name]

    def get_intermediate_storage_def_snap(self, intermedidate_storage_name):
        intermediate_storage_snaps_by_name = {
            intermediate_storage_snap.name: intermediate_storage_snap
            for intermediate_storage_snap in self.intermediate_storage_def_snaps
        }
        return intermediate_storage_snaps_by_name[intermedidate_storage_name]


def build_resource_def_snap(name, resource_def):
    check.str_param(name, "name")
    check.inst_param(resource_def, "resource_def", ResourceDefinition)

    from dagster.core.storage.mem_io_manager import mem_io_manager

    return ResourceDefSnap(
        name=name,
        description=resource_def.description,
        config_field_snap=snap_from_field("config", resource_def.config_field)
        if resource_def.has_config_field
        else None,
        is_system_default=(resource_def == mem_io_manager),
    )


@whitelist_for_serdes
class ResourceDefSnap(
    namedtuple("_ResourceDefSnap", "name description config_field_snap is_system_default")
):
    def __new__(cls, name, description, config_field_snap, is_system_default=None):
        return super(ResourceDefSnap, cls).__new__(
            cls,
            name=check.str_param(name, "name"),
            description=check.opt_str_param(description, "description"),
            config_field_snap=check.opt_inst_param(
                config_field_snap, "config_field_snap", ConfigFieldSnap
            ),
            is_system_default=check.opt_bool_param(is_system_default, "system_default"),
        )


def build_logger_def_snap(name, logger_def):
    check.str_param(name, "name")
    check.inst_param(logger_def, "logger_def", LoggerDefinition)
    return LoggerDefSnap(
        name=name,
        description=logger_def.description,
        config_field_snap=snap_from_field("config", logger_def.config_field)
        if logger_def.has_config_field
        else None,
    )


@whitelist_for_serdes
class LoggerDefSnap(namedtuple("_LoggerDefSnap", "name description config_field_snap")):
    def __new__(cls, name, description, config_field_snap):
        return super(LoggerDefSnap, cls).__new__(
            cls,
            name=check.str_param(name, "name"),
            description=check.opt_str_param(description, "description"),
            config_field_snap=check.opt_inst_param(
                config_field_snap, "config_field_snap", ConfigFieldSnap
            ),
        )
