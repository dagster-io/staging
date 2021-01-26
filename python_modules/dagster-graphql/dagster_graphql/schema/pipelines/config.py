from collections import namedtuple

import graphene
from dagster import check
from dagster.config.errors import EvaluationError as DagsterEvaluationError
from dagster.config.errors import (
    FieldNotDefinedErrorData,
    FieldsNotDefinedErrorData,
    MissingFieldErrorData,
    MissingFieldsErrorData,
    RuntimeMismatchErrorData,
    SelectorTypeErrorData,
)
from dagster.config.snap import ConfigSchemaSnapshot
from dagster.config.stack import EvaluationStackListItemEntry as DagsterEvaluationStackListItemEntry
from dagster.config.stack import EvaluationStackPathEntry as DagsterEvaluationStackPathEntry
from dagster.core.host_representation.represented import RepresentedPipeline
from dagster.utils.error import SerializableErrorInfo

from ..config_types import ConfigTypeField
from ..util import non_null_list


class EvaluationStackListItemEntry(graphene.ObjectType):
    def __init__(self, list_index):
        super(EvaluationStackListItemEntry, self).__init__()
        self._list_index = list_index

    list_index = graphene.NonNull(graphene.Int)

    def resolve_list_index(self, _info):
        return self._list_index


class EvaluationStackPathEntry(graphene.ObjectType):
    def __init__(self, field_name):
        self._field_name = check.str_param(field_name, "field_name")
        super().__init__()

    field_name = graphene.NonNull(graphene.String)

    def resolve_field_name(self, _info):
        return self._field_name


class EvaluationStackEntry(graphene.Union):
    class Meta:
        types = (EvaluationStackListItemEntry, EvaluationStackPathEntry)

    @staticmethod
    def from_native_entry(entry):
        if isinstance(entry, DagsterEvaluationStackPathEntry):
            return EvaluationStackPathEntry(field_name=entry.field_name)
        elif isinstance(entry, DagsterEvaluationStackListItemEntry):
            return EvaluationStackListItemEntry(list_index=entry.list_index)
        else:
            check.failed(f"Unsupported stack entry type {entry}")


class EvaluationStack(graphene.ObjectType):
    def __init__(self, config_schema_snapshot, stack):
        self._config_schema_snapshot = check.inst_param(
            config_schema_snapshot, "config_schema_snapshot", ConfigSchemaSnapshot
        )
        self._stack = stack
        super().__init__()

    entries = non_null_list(lambda: EvaluationStackEntry)

    def resolve_entries(self, _):
        return map(EvaluationStackEntry.from_native_entry, self._stack.entries)


class EvaluationErrorReason(graphene.Enum):
    RUNTIME_TYPE_MISMATCH = "RUNTIME_TYPE_MISMATCH"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    MISSING_REQUIRED_FIELDS = "MISSING_REQUIRED_FIELDS"
    FIELD_NOT_DEFINED = "FIELD_NOT_DEFINED"
    FIELDS_NOT_DEFINED = "FIELDS_NOT_DEFINED"
    SELECTOR_FIELD_ERROR = "SELECTOR_FIELD_ERROR"


class PipelineConfigValidationError(graphene.Interface):
    message = graphene.NonNull(graphene.String)
    path = non_null_list(graphene.String)
    stack = graphene.NonNull(EvaluationStack)
    reason = graphene.NonNull(EvaluationErrorReason)

    @staticmethod
    def from_dagster_error(config_schema_snapshot, error):
        check.inst_param(config_schema_snapshot, "config_schema_snapshot", ConfigSchemaSnapshot)
        check.inst_param(error, "error", DagsterEvaluationError)

        if isinstance(error.error_data, RuntimeMismatchErrorData):
            return RuntimeMismatchConfigError(
                message=error.message,
                path=[],  # TODO: remove
                stack=EvaluationStack(config_schema_snapshot, error.stack),
                reason=error.reason,
                value_rep=error.error_data.value_rep,
            )
        elif isinstance(error.error_data, MissingFieldErrorData):
            return MissingFieldConfigError(
                message=error.message,
                path=[],  # TODO: remove
                stack=EvaluationStack(config_schema_snapshot, error.stack),
                reason=error.reason,
                field=ConfigTypeField(
                    config_schema_snapshot=config_schema_snapshot,
                    field_snap=error.error_data.field_snap,
                ),
            )
        elif isinstance(error.error_data, MissingFieldsErrorData):
            return MissingFieldsConfigError(
                message=error.message,
                path=[],  # TODO: remove
                stack=EvaluationStack(config_schema_snapshot, error.stack),
                reason=error.reason,
                fields=[
                    ConfigTypeField(
                        config_schema_snapshot=config_schema_snapshot, field_snap=field_snap,
                    )
                    for field_snap in error.error_data.field_snaps
                ],
            )

        elif isinstance(error.error_data, FieldNotDefinedErrorData):
            return FieldNotDefinedConfigError(
                message=error.message,
                path=[],  # TODO: remove
                stack=EvaluationStack(config_schema_snapshot, error.stack),
                reason=error.reason,
                field_name=error.error_data.field_name,
            )
        elif isinstance(error.error_data, FieldsNotDefinedErrorData):
            return FieldsNotDefinedConfigError(
                message=error.message,
                path=[],  # TODO: remove
                stack=EvaluationStack(config_schema_snapshot, error.stack),
                reason=error.reason,
                field_names=error.error_data.field_names,
            )
        elif isinstance(error.error_data, SelectorTypeErrorData):
            return SelectorTypeConfigError(
                message=error.message,
                path=[],  # TODO: remove
                stack=EvaluationStack(config_schema_snapshot, error.stack),
                reason=error.reason,
                incoming_fields=error.error_data.incoming_fields,
            )
        else:
            check.failed(
                "Error type not supported {error_data}".format(error_data=repr(error.error_data))
            )


class RuntimeMismatchConfigError(graphene.ObjectType):
    class Meta:
        interfaces = (PipelineConfigValidationError,)

    value_rep = graphene.Field(graphene.String)


class MissingFieldConfigError(graphene.ObjectType):
    class Meta:
        interfaces = (PipelineConfigValidationError,)

    field = graphene.NonNull(ConfigTypeField)


class MissingFieldsConfigError(graphene.ObjectType):
    class Meta:
        interfaces = (PipelineConfigValidationError,)

    fields = non_null_list(ConfigTypeField)


class FieldNotDefinedConfigError(graphene.ObjectType):
    class Meta:
        interfaces = (PipelineConfigValidationError,)

    field_name = graphene.NonNull(graphene.String)


class FieldsNotDefinedConfigError(graphene.ObjectType):
    class Meta:
        interfaces = (PipelineConfigValidationError,)

    field_names = non_null_list(graphene.String)


class SelectorTypeConfigError(graphene.ObjectType):
    class Meta:
        interfaces = (PipelineConfigValidationError,)

    incoming_fields = non_null_list(graphene.String)


ERROR_DATA_TYPES = (
    FieldNotDefinedErrorData,
    FieldsNotDefinedErrorData,
    MissingFieldErrorData,
    MissingFieldsErrorData,
    RuntimeMismatchErrorData,
    SelectorTypeErrorData,
    SerializableErrorInfo,
)


class EvaluationError(namedtuple("_EvaluationError", "stack reason message error_data")):
    def __new__(cls, stack, reason, message, error_data):
        return super().__new__(
            cls,
            check.inst_param(stack, "stack", EvaluationStack),
            check.inst_param(reason, "reason", EvaluationErrorReason),
            check.str_param(message, "message"),
            check.inst_param(error_data, "error_data", ERROR_DATA_TYPES),
        )


class PipelineConfigValidationValid(graphene.ObjectType):
    pipeline_name = graphene.NonNull(graphene.String)


class PipelineConfigValidationInvalid(graphene.ObjectType):
    pipeline_name = graphene.NonNull(graphene.String)
    errors = non_null_list(PipelineConfigValidationError)

    @staticmethod
    def for_validation_errors(represented_pipeline, errors):
        check.inst_param(represented_pipeline, "represented_pipeline", RepresentedPipeline)
        check.list_param(errors, "errors", of_type=DagsterEvaluationError)
        return PipelineConfigValidationInvalid(
            pipeline_name=represented_pipeline.name,
            errors=[
                PipelineConfigValidationError.from_dagster_error(
                    represented_pipeline.config_schema_snapshot, err
                )
                for err in errors
            ],
        )
