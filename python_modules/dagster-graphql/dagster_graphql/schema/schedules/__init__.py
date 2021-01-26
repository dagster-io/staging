import graphene
from dagster import check
from dagster.core.host_representation import ExternalSchedule, ScheduleSelector
from dagster.core.host_representation.selector import RepositorySelector
from dagster.core.scheduler.job import JobTickStatsSnapshot, JobTickStatus
from dagster.core.storage.pipeline_run import PipelineRunsFilter

from ...implementation.fetch_schedules import (
    reconcile_scheduler_state,
    start_schedule,
    stop_schedule,
)
from ..errors import (
    PythonError,
    RepositoryNotFoundError,
    ScheduleNotFoundError,
    SchedulerNotDefinedError,
)
from ..jobs import JobState
from .schedules import Schedule, ScheduleOrError, Schedules, SchedulesOrError


class ScheduleStatus(graphene.Enum):
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    ENDED = "ENDED"


class Scheduler(graphene.ObjectType):
    scheduler_class = graphene.String()


class SchedulerOrError(graphene.Union):
    class Meta:
        types = (Scheduler, SchedulerNotDefinedError, PythonError)


JobTickStatus = graphene.Enum.from_enum(JobTickStatus)


class ScheduleTickStatsSnapshot(graphene.ObjectType):
    ticks_started = graphene.NonNull(graphene.Int)
    ticks_succeeded = graphene.NonNull(graphene.Int)
    ticks_skipped = graphene.NonNull(graphene.Int)
    ticks_failed = graphene.NonNull(graphene.Int)

    def __init__(self, stats):
        super().__init__(
            ticks_started=stats.ticks_started,
            ticks_succeeded=stats.ticks_succeeded,
            ticks_skipped=stats.ticks_skipped,
            ticks_failed=stats.ticks_failed,
        )
        self._stats = check.inst_param(stats, "stats", JobTickStatsSnapshot)


class ReconcileSchedulerStateSuccess(graphene.ObjectType):
    message = graphene.NonNull(graphene.String)


class ReconcileSchedulerStateMutationResult(graphene.Union):
    class Meta:
        types = (PythonError, ReconcileSchedulerStateSuccess)


class ReconcileSchedulerStateMutation(graphene.Mutation):
    class Arguments:
        repository_selector = graphene.NonNull(RepositorySelector)

    Output = graphene.NonNull(ReconcileSchedulerStateMutationResult)

    def mutate(self, graphene_info, repository_selector):
        return reconcile_scheduler_state(
            graphene_info, RepositorySelector.from_graphql_input(repository_selector)
        )


class ScheduleStateResult(graphene.ObjectType):
    scheduleState = graphene.NonNull(JobState)


class ScheduleMutationResult(graphene.Union):
    class Meta:
        types = (PythonError, ScheduleStateResult)


class StartScheduleMutation(graphene.Mutation):
    class Arguments:
        schedule_selector = graphene.NonNull(ScheduleSelector)

    Output = graphene.NonNull(ScheduleMutationResult)

    def mutate(self, graphene_info, schedule_selector):
        return start_schedule(graphene_info, ScheduleSelector.from_graphql_input(schedule_selector))


class StopRunningScheduleMutation(graphene.Mutation):
    class Arguments:
        schedule_origin_id = graphene.NonNull(graphene.String)

    Output = graphene.NonNull(ScheduleMutationResult)

    def mutate(self, graphene_info, schedule_origin_id):
        return stop_schedule(graphene_info, schedule_origin_id)
