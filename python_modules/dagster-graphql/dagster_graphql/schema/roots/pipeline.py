import graphene

from ..errors import PipelineNotFoundError, PythonError
from ..pipelines.pipeline import Pipeline
from ..pipelines.pipeline_errors import InvalidSubsetError


class PipelineOrError(graphene.Union):
    class Meta:
        types = (Pipeline, PipelineNotFoundError, InvalidSubsetError, PythonError)
