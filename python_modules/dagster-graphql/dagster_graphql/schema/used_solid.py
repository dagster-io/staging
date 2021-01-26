import graphene

from .pipelines.pipeline import Pipeline
from .solids import ISolidDefinition, SolidHandle
from .util import non_null_list


class SolidInvocationSite(graphene.ObjectType):
    class Meta:
        description = """An invocation of a solid within a repo."""

    pipeline = graphene.NonNull(Pipeline)
    solidHandle = graphene.NonNull(SolidHandle)


class UsedSolid(graphene.ObjectType):
    class Meta:
        description = """A solid definition and its invocations within the repo."""

    definition = graphene.NonNull(ISolidDefinition)
    invocations = non_null_list(SolidInvocationSite)
