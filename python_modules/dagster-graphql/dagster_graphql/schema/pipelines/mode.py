import graphene
from dagster import check
from dagster.core.snap import ConfigSchemaSnapshot, ModeDefSnap

from ..util import non_null_list
from .logger import Logger
from .resource import Resource


class Mode(graphene.ObjectType):
    def __init__(self, config_schema_snapshot, mode_def_snap):
        super().__init__()
        self._mode_def_snap = check.inst_param(mode_def_snap, "mode_def_snap", ModeDefSnap)
        self._config_schema_snapshot = check.inst_param(
            config_schema_snapshot, "config_schema_snapshot", ConfigSchemaSnapshot
        )

    name = graphene.NonNull(graphene.String)
    description = graphene.String()
    resources = non_null_list(Resource)
    loggers = non_null_list(Logger)

    def resolve_name(self, _graphene_info):
        return self._mode_def_snap.name

    def resolve_description(self, _graphene_info):
        return self._mode_def_snap.description

    def resolve_resources(self, _graphene_info):
        return [
            Resource(self._config_schema_snapshot, resource_def_snap)
            for resource_def_snap in sorted(self._mode_def_snap.resource_def_snaps)
        ]

    def resolve_loggers(self, _graphene_info):
        return [
            Logger(self._config_schema_snapshot, logger_def_snap)
            for logger_def_snap in sorted(self._mode_def_snap.logger_def_snaps)
        ]
