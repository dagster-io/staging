from dagster import check
from dagster.core.definitions.job import JobType
from dagster.core.scheduler.job import JobStatus, JobTick, JobTickStatus
from dagster_graphql import dauphin
from dagster_graphql.schema.errors import (
    DauphinJobDefinitionNotFoundError,
    DauphinPythonError,
    DauphinRepositoryNotFoundError,
)


class DauphinJobDefinition(dauphin.Union):
    class Meta:
        name = "JobDefinition"
        types = (
            "ScheduleDefinition",
            "SensorDefinition",
        )


class DauphinJobDefinitionOrError(dauphin.Union):
    class Meta:
        name = "JobDefinitionOrError"
        types = (
            "ScheduleDefinition",
            "SensorDefinition",
            DauphinJobDefinitionNotFoundError,
            DauphinPythonError,
        )


class DauphinJobDefinitions(dauphin.ObjectType):
    class Meta:
        name = "JobDefinitions"

    results = dauphin.non_null_list("JobDefinition")


class DauphinJobDefintionsOrError(dauphin.Union):
    class Meta:
        name = "JobDefinitionsOrError"
        types = (DauphinJobDefinitions, DauphinRepositoryNotFoundError, DauphinPythonError)


class DauphinJobTick(dauphin.ObjectType):
    class Meta:
        name = "JobTick"

    status = dauphin.NonNull("JobTickStatus")
    timestamp = dauphin.NonNull(dauphin.Float)
    runId = dauphin.String()
    error = dauphin.Field("PythonError")
    execution_key = dauphin.String()

    run = dauphin.Field("PipelineRun")

    def __init__(self, _, job_tick):
        self._job_tick = check.inst_param(job_tick, "job_tick", JobTick)

        super(DauphinJobTick, self).__init__(
            status=job_tick.status,
            timestamp=job_tick.timestamp,
            runId=job_tick.run_id,
            error=job_tick.error,
            execution_key=job_tick.execution_key,
        )

    def resolve_run(self, graphene_info):
        if not self._job_tick.run_id:
            return None

        if not graphene_info.context.instance.has_run(self._job_tick.run_id):
            return None

        return graphene_info.schema.type_named("PipelineRun")(
            graphene_info.context.instance.get_run_by_id(self._job_tick.run_id)
        )


DauphinJobType = dauphin.Enum.from_enum(JobType)
DauphinJobStatus = dauphin.Enum.from_enum(JobStatus)
DauphinJobTickStatus = dauphin.Enum.from_enum(JobTickStatus)
