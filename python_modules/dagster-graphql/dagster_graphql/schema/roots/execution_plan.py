import graphene

from ..errors import PipelineNotFoundError, PythonError
from ..execution import ExecutionPlan
from ..pipelines.config import PipelineConfigValidationInvalid
from ..pipelines.pipeline_errors import InvalidSubsetError


class ExecutionPlanOrError(graphene.Union):
    class Meta:
        types = (
            ExecutionPlan,
            PipelineConfigValidationInvalid,
            PipelineNotFoundError,
            InvalidSubsetError,
            PythonError,
        )
