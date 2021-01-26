import graphene
from dagster.core.storage.tags import TagType as DagsterTagType

from .util import non_null_list

TagType = graphene.Enum.from_enum(DagsterTagType)


class PipelineTag(graphene.ObjectType):
    key = graphene.NonNull(graphene.String)
    value = graphene.NonNull(graphene.String)

    def __init__(self, key, value):
        super(PipelineTag, self).__init__(key=key, value=value)


class PipelineTagAndValues(graphene.ObjectType):
    class Meta:
        description = """A run tag and the free-form values that have been associated
        with it so far."""

    key = graphene.NonNull(graphene.String)
    values = non_null_list(graphene.String)

    def __init__(self, key, values):
        super(PipelineTagAndValues, self).__init__(key=key, values=values)
