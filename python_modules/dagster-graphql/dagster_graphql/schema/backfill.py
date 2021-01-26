import graphene

from .errors import (
    InvalidOutputError,
    InvalidStepError,
    PartitionSetNotFoundError,
    PipelineNotFoundError,
    PipelineRunConflict,
    PythonError,
    create_execution_params_error_types,
)
from .pipelines.config import PipelineConfigValidationInvalid
from .util import non_null_list

pipeline_execution_error_types = (
    InvalidStepError,
    InvalidOutputError,
    PipelineConfigValidationInvalid,
    PipelineNotFoundError,
    PipelineRunConflict,
    PythonError,
) + create_execution_params_error_types


class PartitionBackfillSuccess(graphene.ObjectType):
    backfill_id = graphene.NonNull(graphene.String)
    launched_run_ids = non_null_list(graphene.String)


class PartitionBackfillResult(graphene.Union):
    class Meta:
        types = (
            PartitionBackfillSuccess,
            PartitionSetNotFoundError,
        ) + pipeline_execution_error_types
