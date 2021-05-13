import graphene
from dagster import check
from dagster.core.storage.compute_log_manager import ComputeIOType
from rx import Observable

from ...implementation.execution import get_pipeline_run_observable
from ...schema.logs.compute_logs import from_compute_log_file
from ..external import GrapheneLocationStateChangeSubscription, get_location_state_change_observable
from ..logs.compute_logs import GrapheneComputeIOType, GrapheneComputeLogFile
from ..paging import GrapheneCursor
from ..pipelines.subscription import GraphenePipelineRunLogsSubscriptionPayload


class GrapheneSubscription(graphene.ObjectType):
    pipelineRunLogs = graphene.Field(
        graphene.NonNull(GraphenePipelineRunLogsSubscriptionPayload),
        runId=graphene.Argument(graphene.NonNull(graphene.ID)),
        after=graphene.Argument(GrapheneCursor),
    )

    computeLogs = graphene.Field(
        graphene.NonNull(GrapheneComputeLogFile),
        runId=graphene.Argument(graphene.NonNull(graphene.ID)),
        stepKey=graphene.Argument(graphene.NonNull(graphene.String)),
        ioType=graphene.Argument(graphene.NonNull(GrapheneComputeIOType)),
        cursor=graphene.Argument(graphene.String),
    )

    locationStateChangeEvents = graphene.Field(
        graphene.NonNull(GrapheneLocationStateChangeSubscription)
    )

    class Meta:
        name = "Subscription"

    def resolve_pipelineRunLogs(self, graphene_info, runId, after=None):
        return get_pipeline_run_observable(graphene_info, runId, after)

    def resolve_computeLogs(self, graphene_info, runId, stepKey, ioType, cursor=None):
        check.str_param(ioType, "ioType")  # need to resolve to enum
        compute_log_manager = graphene_info.context.instance.compute_log_manager
        if compute_log_manager.use_legacy_api:
            return compute_log_manager.observable(
                runId, stepKey, ComputeIOType(ioType), cursor
            ).map(lambda update: from_compute_log_file(graphene_info, update))

        subscription = compute_log_manager.subscribe(stepKey, runId)
        return Observable.create(subscription).map(  # pylint: disable=E1101
            lambda update: from_compute_log_file(graphene_info, update)
        )

    def resolve_locationStateChangeEvents(self, graphene_info):
        return get_location_state_change_observable(graphene_info)
