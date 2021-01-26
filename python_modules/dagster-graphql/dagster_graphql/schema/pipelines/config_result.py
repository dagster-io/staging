import graphene

from ..errors import PipelineNotFoundError, PythonError
from .config import PipelineConfigValidationInvalid, PipelineConfigValidationValid
from .pipeline_errors import InvalidSubsetError


class PipelineConfigValidationResult(graphene.Union):
    class Meta:
        types = (
            InvalidSubsetError,
            PipelineConfigValidationValid,
            PipelineConfigValidationInvalid,
            PipelineNotFoundError,
            PythonError,
        )
