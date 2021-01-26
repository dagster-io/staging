import graphene
from dagster import check

from ...implementation.execution import get_compute_log_observable, get_pipeline_run_observable
from ..external import LocationStateChangeSubscription, get_location_state_change_observable
from ..logs.compute_logs import ComputeIOType, ComputeLogFile
from ..paging import Cursor
from ..pipelines.subscription import PipelineRunLogsSubscriptionPayload


class Subscription(graphene.ObjectType):
    pipelineRunLogs = graphene.Field(
        graphene.NonNull(PipelineRunLogsSubscriptionPayload),
        runId=graphene.Argument(graphene.NonNull(graphene.ID)),
        after=graphene.Argument(Cursor),
    )

    computeLogs = graphene.Field(
        graphene.NonNull(ComputeLogFile),
        runId=graphene.Argument(graphene.NonNull(graphene.ID)),
        stepKey=graphene.Argument(graphene.NonNull(graphene.String)),
        ioType=graphene.Argument(graphene.NonNull(ComputeIOType)),
        cursor=graphene.Argument(graphene.String),
    )

    locationStateChangeEvents = graphene.Field(graphene.NonNull(LocationStateChangeSubscription))

    def resolve_pipelineRunLogs(self, graphene_info, runId, after=None):
        return get_pipeline_run_observable(graphene_info, runId, after)

    def resolve_computeLogs(self, graphene_info, runId, stepKey, ioType, cursor=None):
        check.str_param(ioType, "ioType")  # need to resolve to enum
        return get_compute_log_observable(
            graphene_info, runId, stepKey, ComputeIOType(ioType), cursor
        )

    def resolve_locationStateChangeEvents(self, graphene_info):
        return get_location_state_change_observable(graphene_info)
