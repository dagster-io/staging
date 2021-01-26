import graphene

from .config_types import CompositeConfigType, EnumConfigType, RegularConfigType
from .errors import PipelineNotFoundError, PythonError
from .pipelines.pipeline_errors import ConfigTypeNotFoundError


class ConfigTypeOrError(graphene.Union):
    class Meta:

        types = (
            EnumConfigType,
            CompositeConfigType,
            RegularConfigType,
            PipelineNotFoundError,
            ConfigTypeNotFoundError,
            PythonError,
        )
