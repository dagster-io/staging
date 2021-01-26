import graphene

from ..logs.events import PipelineRunEvent
from ..util import non_null_list
from .pipeline import PipelineRun


class PipelineRunLogsSubscriptionSuccess(graphene.ObjectType):
    run = graphene.NonNull(PipelineRun)
    messages = non_null_list(PipelineRunEvent)


class PipelineRunLogsSubscriptionFailure(graphene.ObjectType):
    message = graphene.NonNull(graphene.String)
    missingRunId = graphene.Field(graphene.String)


class PipelineRunLogsSubscriptionPayload(graphene.Union):
    class Meta:
        types = (
            PipelineRunLogsSubscriptionSuccess,
            PipelineRunLogsSubscriptionFailure,
        )
