import graphene
from dagster import check
from dagster.core.snap import ConfigSchemaSnapshot, LoggerDefSnap

from ..config_types import ConfigTypeField


class Logger(graphene.ObjectType):
    def __init__(self, config_schema_snapshot, logger_def_snap):
        super().__init__()
        self._config_schema_snapshot = check.inst_param(
            config_schema_snapshot, "config_schema_snapshot", ConfigSchemaSnapshot
        )
        self._logger_def_snap = check.inst_param(logger_def_snap, "logger_def_snap", LoggerDefSnap)
        self.name = logger_def_snap.name
        self.description = logger_def_snap.description

    name = graphene.NonNull(graphene.String)
    description = graphene.String()
    configField = graphene.Field(ConfigTypeField)

    def resolve_configField(self, _):
        return (
            ConfigTypeField(
                config_schema_snapshot=self._config_schema_snapshot,
                field_snap=self._logger_def_snap.config_field_snap,
            )
            if self._logger_def_snap.config_field_snap
            else None
        )
