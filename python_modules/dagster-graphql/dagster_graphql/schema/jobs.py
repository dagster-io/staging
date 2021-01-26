import graphene
import pendulum
import yaml
from dagster import check
from dagster.core.definitions.job import JobType
from dagster.core.definitions.job import RunRequest as DagsterRunRequest
from dagster.core.host_representation import (
    ExternalScheduleExecutionData,
    ExternalScheduleExecutionErrorData,
)
from dagster.core.scheduler.job import JobState as DagsterJobState
from dagster.core.scheduler.job import JobStatus as DagsterJobStatus
from dagster.core.scheduler.job import JobTick as DagsterJobTick
from dagster.core.scheduler.job import JobTickStatus as DagsterJobTickStatus
from dagster.core.scheduler.job import JobType as DagsterJobType
from dagster.core.scheduler.job import ScheduleJobData as DagsterScheduleJobData
from dagster.core.scheduler.job import SensorJobData as DagsterSensorJobData
from dagster.core.storage.pipeline_run import PipelineRunsFilter
from dagster.core.storage.tags import TagType, get_tag_type

from .errors import PythonError
from .repository_origin import RepositoryOrigin
from .tags import PipelineTag
from .util import non_null_list

JobType = graphene.Enum.from_enum(DagsterJobType)
JobStatus = graphene.Enum.from_enum(DagsterJobStatus)
JobTickStatus = graphene.Enum.from_enum(DagsterJobTickStatus)


class SensorJobData(graphene.ObjectType):
    lastTickTimestamp = graphene.Float()
    lastRunKey = graphene.String()

    def __init__(self, job_specific_data):
        check.inst_param(job_specific_data, "job_specific_data", DagsterSensorJobData)
        super().__init__(
            lastTickTimestamp=job_specific_data.last_tick_timestamp,
            lastRunKey=job_specific_data.last_run_key,
        )


class ScheduleJobData(graphene.ObjectType):
    cronSchedule = graphene.NonNull(graphene.String)
    startTimestamp = graphene.Float()

    def __init__(self, job_specific_data):
        check.inst_param(job_specific_data, "job_specific_data", DagsterScheduleJobData)
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

    runs = non_null_list("dagster_graphql.schema.pipelines.pipeline.PipelineRun")

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
        from .pipelines.pipeline import PipelineRun

        instance = graphene_info.context.instance
        return [
            PipelineRun(instance.get_run_by_id(run_id))
            for run_id in self._job_tick.run_ids
            if instance.has_run(run_id)
        ]


class FutureJobTick(graphene.ObjectType):
    timestamp = graphene.NonNull(graphene.Float)
    evaluationResult = graphene.Field("TickEvaluation")

    def __init__(self, job_state, timestamp):
        self._job_state = check.inst_param(job_state, "job_state", DagsterJobState)
        self._timestamp = timestamp
        super().__init__(
            timestamp=check.float_param(timestamp, "timestamp"),
        )

    def resolve_evaluationResult(self, graphene_info):
        if self._job_state.status != DagsterJobStatus.RUNNING:
            return None

        if self._job_state.job_type != DagsterJobType.SCHEDULE:
            return None

        repository_origin = self._job_state.origin.external_repository_origin
        if not graphene_info.context.has_repository_location(
            repository_origin.repository_location_origin.location_name
        ):
            return None

        repository_location = graphene_info.context.get_repository_location(
            repository_origin.repository_location_origin.location_name
        )
        if not repository_location.has_repository(repository_origin.repository_name):
            return None

        repository = repository_location.get_repository(repository_origin.repository_name)
        external_schedule = repository.get_external_job(self._job_state.name)
        timezone_str = external_schedule.execution_timezone
        if not timezone_str:
            timezone_str = pendulum.now().timezone.name

        next_tick_datetime = next(external_schedule.execution_time_iterator(self._timestamp))
        schedule_time = pendulum.instance(next_tick_datetime).in_tz(timezone_str)
        schedule_data = repository_location.get_external_schedule_execution_data(
            instance=graphene_info.context.instance,
            repository_handle=repository.handle,
            schedule_name=external_schedule.name,
            scheduled_execution_time=schedule_time,
        )
        return TickEvaluation(schedule_data)


class TickEvaluation(graphene.ObjectType):
    runRequests = graphene.List(lambda: RunRequest)
    skipReason = graphene.String()
    error = graphene.Field(PythonError)

    def __init__(self, schedule_data):
        check.inst_param(
            schedule_data,
            "schedule_data",
            (ExternalScheduleExecutionData, ExternalScheduleExecutionErrorData),
        )
        error = (
            schedule_data.error
            if isinstance(schedule_data, ExternalScheduleExecutionErrorData)
            else None
        )
        skip_reason = (
            schedule_data.skip_message
            if isinstance(schedule_data, ExternalScheduleExecutionData)
            else None
        )
        self._run_requests = (
            schedule_data.run_requests
            if isinstance(schedule_data, ExternalScheduleExecutionData)
            else None
        )
        super().__init__(skipReason=skip_reason, error=error)

    def resolve_runRequests(self, graphene_info):
        if not self._run_requests:
            return self._run_requests

        return [RunRequest(run_request) for run_request in self._run_requests]


class RunRequest(graphene.ObjectType):
    runKey = graphene.String()
    tags = non_null_list(PipelineTag)
    runConfigYaml = graphene.NonNull(graphene.String)

    def __init__(self, run_request):
        super().__init__(runKey=run_request.run_key)
        self._run_request = check.inst_param(run_request, "run_request", DagsterRunRequest)

    def resolve_tags(self, graphene_info):
        return [
            PipelineTag(key=key, value=value)
            for key, value in self._run_request.tags.items()
            if get_tag_type(key) != TagType.HIDDEN
        ]

    def resolve_runConfigYaml(self, _graphene_info):
        return yaml.dump(self._run_request.run_config, default_flow_style=False, allow_unicode=True)


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
    runs = graphene.Field(
        non_null_list("dagster_graphql.schema.pipelines.pipeline.PipelineRun"), limit=graphene.Int()
    )
    runsCount = graphene.NonNull(graphene.Int)
    ticks = graphene.Field(non_null_list(JobTick), limit=graphene.Int())
    runningCount = graphene.NonNull(graphene.Int)  # remove with cron scheduler

    def __init__(self, job_state):
        self._job_state = check.inst_param(job_state, "job_state", DagsterJobState)
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

        if self._job_state.job_type == DagsterJobType.SENSOR:
            return SensorJobData(self._job_state.job_specific_data)

        if self._job_state.job_type == DagsterJobType.SCHEDULE:
            return ScheduleJobData(self._job_state.job_specific_data)

        return None

    def resolve_runs(self, graphene_info, **kwargs):
        from .pipelines.pipeline import PipelineRun

        if self._job_state.job_type == DagsterJobType.SENSOR:
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
        if self._job_state.job_type == DagsterJobType.SENSOR:
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
        if self._job_state.job_type == DagsterJobType.SENSOR:
            return 1 if self._job_state.status == DagsterJobStatus.RUNNING else 0
        else:
            return graphene_info.context.instance.running_schedule_count(
                self._job_state.job_origin_id
            )


class JobStates(graphene.ObjectType):
    results = non_null_list(JobState)


class JobStatesOrError(graphene.Union):
    class Meta:
        types = (JobStates, PythonError)


types = [
    FutureJobTick,
    FutureJobTicks,
    JobSpecificData,
    JobState,
    JobStates,
    JobStatesOrError,
    JobTick,
    ScheduleJobData,
    SensorJobData,
]
