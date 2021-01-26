import graphene
from dagster import check

from ..errors import Error
from .pipeline import Pipeline


class InvalidSubsetError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    pipeline = graphene.Field(graphene.NonNull(Pipeline))

    def __init__(self, message, pipeline):
        super().__init__()
        self.message = check.str_param(message, "message")
        self.pipeline = pipeline


class ConfigTypeNotFoundError(graphene.ObjectType):
    class Meta:
        interfaces = (Error,)

    pipeline = graphene.NonNull(Pipeline)
    config_type_name = graphene.NonNull(graphene.String)
