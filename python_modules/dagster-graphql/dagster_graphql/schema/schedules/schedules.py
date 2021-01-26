import graphene
import pendulum
from dagster import check
from dagster.core.host_representation import ExternalSchedule
from dagster.seven import get_current_datetime_in_utc, get_timestamp_from_utc_datetime

from ..errors import PythonError, RepositoryNotFoundError, ScheduleNotFoundError
from ..jobs import FutureJobTick, FutureJobTicks, JobState
from ..partition_sets import PartitionSet
from ..util import non_null_list


class Schedule(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    name = graphene.NonNull(graphene.String)
    cron_schedule = graphene.NonNull(graphene.String)
    pipeline_name = graphene.NonNull(graphene.String)
    solid_selection = graphene.List(graphene.String)
    mode = graphene.NonNull(graphene.String)
    execution_timezone = graphene.Field(graphene.String)
    scheduleState = graphene.NonNull(JobState)
    partition_set = graphene.Field(PartitionSet)

    futureTicks = graphene.NonNull(FutureJobTicks, cursor=graphene.Float(), limit=graphene.Int())

    def resolve_id(self, _):
        return "%s:%s" % (self.name, self.pipeline_name)

    def resolve_partition_set(self, graphene_info):
        if self._external_schedule.partition_set_name is None:
            return None

        repository = graphene_info.context.get_repository_location(
            self._external_schedule.handle.location_name
        ).get_repository(self._external_schedule.handle.repository_name)
        external_partition_set = repository.get_external_partition_set(
            self._external_schedule.partition_set_name
        )

        return PartitionSet(
            external_repository_handle=repository.handle,
            external_partition_set=external_partition_set,
        )

    def resolve_futureTicks(self, graphene_info, **kwargs):
        cursor = kwargs.get(
            "cursor", get_timestamp_from_utc_datetime(get_current_datetime_in_utc())
        )
        limit = kwargs.get("limit", 10)

        tick_times = []
        time_iter = self._external_schedule.execution_time_iterator(cursor)

        for _ in range(limit):
            tick_times.append(next(time_iter).timestamp())

        future_ticks = [FutureJobTick(tick_time) for tick_time in tick_times]

        return FutureJobTicks(results=future_ticks, cursor=tick_times[-1] + 1)

    def __init__(self, graphene_info, external_schedule):
        self._external_schedule = check.inst_param(
            external_schedule, "external_schedule", ExternalSchedule
        )
        self._schedule_state = graphene_info.context.instance.get_job_state(
            self._external_schedule.get_external_origin_id()
        )

        if not self._schedule_state:
            # Also include a ScheduleState for a stopped schedule that may not
            # have a stored database row yet
            self._schedule_state = self._external_schedule.get_default_job_state(
                graphene_info.context.instance
            )

        super(Schedule, self).__init__(
            name=external_schedule.name,
            cron_schedule=external_schedule.cron_schedule,
            pipeline_name=external_schedule.pipeline_name,
            solid_selection=external_schedule.solid_selection,
            mode=external_schedule.mode,
            scheduleState=JobState(self._schedule_state),
            execution_timezone=(
                self._external_schedule.execution_timezone
                if self._external_schedule.execution_timezone
                else pendulum.now().timezone.name
            ),
        )


class ScheduleOrError(graphene.Union):
    class Meta:
        types = (Schedule, ScheduleNotFoundError, PythonError)


class Schedules(graphene.ObjectType):
    results = non_null_list(Schedule)


class SchedulesOrError(graphene.Union):
    class Meta:
        types = (Schedules, RepositoryNotFoundError, PythonError)
