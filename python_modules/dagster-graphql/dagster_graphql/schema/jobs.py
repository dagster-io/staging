import graphene
from dagster import check
from dagster.core.definitions.job import JobType
from dagster.core.scheduler.job import JobState as DagsterJobState
from dagster.core.scheduler.job import JobStatus as DagsterJobStatus
from dagster.core.scheduler.job import JobTick as DagsterJobTick
from dagster.core.scheduler.job import JobTickStatus as DagsterJobTickStatus
from dagster.core.scheduler.job import JobType as DagsterJobType
from dagster.core.scheduler.job import ScheduleJobData as DagsterScheduleJobData
from dagster.core.scheduler.job import SensorJobData as DagsterSensorJobData
from dagster.core.storage.pipeline_run import PipelineRunsFilter

from .errors import PythonError
from .repository_origin import RepositoryOrigin
from .runs import PipelineRun
from .util import non_null_list

JobType = graphene.Enum.from_enum(DagsterJobType)
JobStatus = graphene.Enum.from_enum(DagsterJobStatus)
JobTickStatus = graphene.Enum.from_enum(DagsterJobTickStatus)


class SensorJobData(graphene.ObjectType):
    lastTickTimestamp = graphene.Float()
    lastRunKey = graphene.String()

    def __init__(self, job_specific_data):
        check.inst_param(job_specific_data, "job_specific_data", SensorJobData)
        super().__init__(
            lastTickTimestamp=job_specific_data.last_tick_timestamp,
            lastRunKey=job_specific_data.last_run_key,
        )


class ScheduleJobData(graphene.ObjectType):
    cronSchedule = graphene.NonNull(graphene.String)
    startTimestamp = graphene.Float()

    def __init__(self, job_specific_data):
        check.inst_param(job_specific_data, "job_specific_data", ScheduleJobData)
        super().__init__(
            cronSchedule=job_specific_data.cron_schedule,
            startTimestamp=job_specific_data.start_timestamp,
        )


class JobSpecificData(graphene.Union):
    class Meta:
        types = (SensorJobData, ScheduleJobData)


class JobTick(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    status = graphene.NonNull(JobTickStatus)
    timestamp = graphene.NonNull(graphene.Float)
    runIds = non_null_list(graphene.String)
    error = graphene.Field(PythonError)
    skipReason = graphene.String()

    runs = non_null_list(PipelineRun)

    def __init__(self, _, job_tick):
        self._job_tick = check.inst_param(job_tick, "job_tick", DagsterJobTick)

        super().__init__(
            status=job_tick.status,
            timestamp=job_tick.timestamp,
            runIds=job_tick.run_ids,
            error=job_tick.error,
            skipReason=job_tick.skip_reason,
        )

    def resolve_id(self, _):
        return "%s:%s" % (self._job_tick.job_origin_id, self._job_tick.timestamp)

    def resolve_runs(self, graphene_info):
        instance = graphene_info.context.instance
        return [
            PipelineRun(instance.get_run_by_id(run_id))
            for run_id in self._job_tick.run_ids
            if instance.has_run(run_id)
        ]


class FutureJobTick(graphene.ObjectType):
    timestamp = graphene.NonNull(graphene.Float)

    def __init__(self, timestamp):
        super().__init__(timestamp=check.float_param(timestamp, "timestamp"))


class FutureJobTicks(graphene.ObjectType):
    results = non_null_list(FutureJobTick)
    cursor = graphene.NonNull(graphene.Float)


class JobState(graphene.ObjectType):
    id = graphene.NonNull(graphene.ID)
    name = graphene.NonNull(graphene.String)
    jobType = graphene.NonNull(JobType)
    status = graphene.NonNull(JobStatus)
    repositoryOrigin = graphene.NonNull(RepositoryOrigin)
    jobSpecificData = graphene.Field(JobSpecificData)
    runs = graphene.Field(non_null_list(PipelineRun), limit=graphene.Int())
    runsCount = graphene.NonNull(graphene.Int)
    ticks = graphene.Field(non_null_list(JobTick), limit=graphene.Int())
    runningCount = graphene.NonNull(graphene.Int)  # remove with cron scheduler

    def __init__(self, job_state):
        self._job_state = check.inst_param(job_state, "job_state", JobState)
        super().__init__(
            id=job_state.job_origin_id,
            name=job_state.name,
            jobType=job_state.job_type,
            status=job_state.status,
        )

    def resolve_repositoryOrigin(self, _graphene_info):
        origin = self._job_state.origin.external_repository_origin
        return RepositoryOrigin(origin)

    def resolve_jobSpecificData(self, _graphene_info):
        if not self._job_state.job_specific_data:
            return None

        if self._job_state.job_type == JobType.SENSOR:
            return SensorJobData(self._job_state.job_specific_data)

        if self._job_state.job_type == JobType.SCHEDULE:
            return ScheduleJobData(self._job_state.job_specific_data)

        return None

    def resolve_runs(self, graphene_info, **kwargs):
        if self._job_state.job_type == JobType.SENSOR:
            filters = PipelineRunsFilter.for_sensor(self._job_state)
        else:
            filters = PipelineRunsFilter.for_schedule(self._job_state)
        return [
            PipelineRun(r)
            for r in graphene_info.context.instance.get_runs(
                filters=filters, limit=kwargs.get("limit"),
            )
        ]

    def resolve_runsCount(self, graphene_info):
        if self._job_state.job_type == JobType.SENSOR:
            filters = PipelineRunsFilter.for_sensor(self._job_state)
        else:
            filters = PipelineRunsFilter.for_schedule(self._job_state)
        return graphene_info.context.instance.get_runs_count(filters=filters)

    def resolve_ticks(self, graphene_info, limit=None):
        ticks = graphene_info.context.instance.get_job_ticks(self._job_state.job_origin_id)

        if limit:
            ticks = ticks[:limit]

        return [JobTick(graphene_info, tick) for tick in ticks]

    def resolve_runningCount(self, graphene_info):
        if self._job_state.job_type == JobType.SENSOR:
            return 1 if self._job_state.status == JobStatus.RUNNING else 0
        else:
            return graphene_info.context.instance.running_schedule_count(
                self._job_state.job_origin_id
            )


class JobStates(graphene.ObjectType):
    results = non_null_list(JobState)


class JobStatesOrError(graphene.Union):
    class Meta:
        types = (JobStates, PythonError)
