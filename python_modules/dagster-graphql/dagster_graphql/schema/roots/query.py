import graphene
from dagster import check
from dagster.core.definitions.events import AssetKey
from dagster.core.host_representation import RepositorySelector as DagsterRepositorySelector
from dagster.core.host_representation import ScheduleSelector as DagsterScheduleSelector
from dagster.core.host_representation import SensorSelector as DagsterSensorSelector

from ...implementation.external import (
    fetch_repositories,
    fetch_repository,
    fetch_repository_locations,
)
from ...implementation.fetch_assets import get_asset, get_assets
from ...implementation.fetch_jobs import get_unloadable_job_states_or_error
from ...implementation.fetch_partition_sets import get_partition_set, get_partition_sets_or_error
from ...implementation.fetch_pipelines import (
    get_pipeline_or_error,
    get_pipeline_snapshot_or_error_from_pipeline_selector,
    get_pipeline_snapshot_or_error_from_snapshot_id,
)
from ...implementation.fetch_runs import (
    get_execution_plan,
    get_run_by_id,
    get_run_group,
    get_run_groups,
    get_run_tags,
    validate_pipeline_config,
)
from ...implementation.fetch_schedules import (
    get_schedule_or_error,
    get_scheduler_or_error,
    get_schedules_or_error,
)
from ...implementation.fetch_sensors import get_sensor_or_error, get_sensors_or_error
from ...implementation.run_config_schema import resolve_run_config_schema_or_error
from ...implementation.utils import pipeline_selector_from_graphql
from ..external import RepositoriesOrError, RepositoryLocationsOrError, RepositoryOrError
from ..inputs import (
    AssetKeyInput,
    PipelineRunsFilter,
    PipelineSelector,
    RepositorySelector,
    ScheduleSelector,
    SensorSelector,
)
from ..instance import Instance
from ..jobs import JobStatesOrError, JobType
from ..partition_sets import PartitionSetOrError, PartitionSetsOrError
from ..pipelines.config_result import PipelineConfigValidationResult
from ..pipelines.pipeline import PipelineRunOrError
from ..pipelines.snapshot import PipelineSnapshotOrError
from ..run_config import RunConfigSchemaOrError
from ..runs import (
    PipelineRuns,
    PipelineRunsOrError,
    RunConfigData,
    RunGroupOrError,
    RunGroupsOrError,
)
from ..schedules import ScheduleOrError, SchedulerOrError, SchedulesOrError
from ..sensors import SensorOrError, SensorsOrError
from ..tags import PipelineTagAndValues
from ..util import non_null_list
from .assets import AssetOrError, AssetsOrError
from .execution_plan import ExecutionPlanOrError
from .pipeline import PipelineOrError


class Query(graphene.ObjectType):
    version = graphene.NonNull(graphene.String)

    repositoriesOrError = graphene.NonNull(RepositoriesOrError)
    repositoryOrError = graphene.Field(
        graphene.NonNull(RepositoryOrError),
        repositorySelector=graphene.NonNull(RepositorySelector),
    )

    repositoryLocationsOrError = graphene.NonNull(RepositoryLocationsOrError)

    pipelineOrError = graphene.Field(
        graphene.NonNull(PipelineOrError), params=graphene.NonNull(PipelineSelector)
    )

    pipelineSnapshotOrError = graphene.Field(
        graphene.NonNull(PipelineSnapshotOrError),
        snapshotId=graphene.String(),
        activePipelineSelector=graphene.Argument(PipelineSelector),
    )

    scheduler = graphene.Field(graphene.NonNull(SchedulerOrError))

    scheduleOrError = graphene.Field(
        graphene.NonNull(ScheduleOrError), schedule_selector=graphene.NonNull(ScheduleSelector),
    )
    schedulesOrError = graphene.Field(
        graphene.NonNull(SchedulesOrError), repositorySelector=graphene.NonNull(RepositorySelector),
    )
    sensorOrError = graphene.Field(
        graphene.NonNull(SensorOrError), sensorSelector=graphene.NonNull(SensorSelector),
    )
    sensorsOrError = graphene.Field(
        graphene.NonNull(SensorsOrError), repositorySelector=graphene.NonNull(RepositorySelector),
    )
    unloadableJobStatesOrError = graphene.Field(
        graphene.NonNull(JobStatesOrError), jobType=graphene.Argument(JobType)
    )

    partitionSetsOrError = graphene.Field(
        graphene.NonNull(PartitionSetsOrError),
        repositorySelector=graphene.NonNull(RepositorySelector),
        pipelineName=graphene.NonNull(graphene.String),
    )
    partitionSetOrError = graphene.Field(
        graphene.NonNull(PartitionSetOrError),
        repositorySelector=graphene.NonNull(RepositorySelector),
        partitionSetName=graphene.String(),
    )

    pipelineRunsOrError = graphene.Field(
        graphene.NonNull(PipelineRunsOrError),
        filter=graphene.Argument(PipelineRunsFilter),
        cursor=graphene.String(),
        limit=graphene.Int(),
    )

    pipelineRunOrError = graphene.Field(
        graphene.NonNull(PipelineRunOrError), runId=graphene.NonNull(graphene.ID)
    )

    pipelineRunTags = non_null_list(PipelineTagAndValues)

    runGroupOrError = graphene.Field(
        graphene.NonNull(RunGroupOrError), runId=graphene.NonNull(graphene.ID)
    )

    runGroupsOrError = graphene.Field(
        graphene.NonNull(RunGroupsOrError),
        filter=graphene.Argument(PipelineRunsFilter),
        cursor=graphene.String(),
        limit=graphene.Int(),
    )

    isPipelineConfigValid = graphene.Field(
        graphene.NonNull(PipelineConfigValidationResult),
        args={
            "pipeline": graphene.Argument(graphene.NonNull(PipelineSelector)),
            "runConfigData": graphene.Argument(RunConfigData),
            "mode": graphene.Argument(graphene.NonNull(graphene.String)),
        },
    )

    executionPlanOrError = graphene.Field(
        graphene.NonNull(ExecutionPlanOrError),
        args={
            "pipeline": graphene.Argument(graphene.NonNull(PipelineSelector)),
            "runConfigData": graphene.Argument(RunConfigData),
            "mode": graphene.Argument(graphene.NonNull(graphene.String)),
        },
    )

    runConfigSchemaOrError = graphene.Field(
        graphene.NonNull(RunConfigSchemaOrError),
        args={
            "selector": graphene.Argument(graphene.NonNull(PipelineSelector)),
            "mode": graphene.Argument(graphene.String),
        },
        description="""Fetch an environment schema given an execution selection and a mode.
        See the descripton on RunConfigSchema for more information.""",
    )

    instance = graphene.NonNull(Instance)
    assetsOrError = graphene.Field(
        graphene.NonNull(AssetsOrError),
        prefixPath=graphene.Argument(graphene.List(graphene.NonNull(graphene.String))),
    )
    assetOrError = graphene.Field(
        graphene.NonNull(AssetOrError), assetKey=graphene.Argument(graphene.NonNull(AssetKeyInput)),
    )

    def resolve_repositoriesOrError(self, graphene_info):
        return fetch_repositories(graphene_info)

    def resolve_repositoryOrError(self, graphene_info, **kwargs):
        return fetch_repository(
            graphene_info,
            DagsterRepositorySelector.from_graphql_input(kwargs.get("repositorySelector")),
        )

    def resolve_repositoryLocationsOrError(self, graphene_info):
        return fetch_repository_locations(graphene_info)

    def resolve_pipelineSnapshotOrError(self, graphene_info, **kwargs):
        snapshot_id_arg = kwargs.get("snapshotId")
        pipeline_selector_arg = kwargs.get("activePipelineSelector")
        check.invariant(
            not (snapshot_id_arg and pipeline_selector_arg),
            "Must only pass one of snapshotId or activePipelineSelector",
        )
        check.invariant(
            snapshot_id_arg or pipeline_selector_arg,
            "Must set one of snapshotId or activePipelineSelector",
        )

        if pipeline_selector_arg:
            pipeline_selector = pipeline_selector_from_graphql(
                graphene_info.context, kwargs["activePipelineSelector"]
            )
            return get_pipeline_snapshot_or_error_from_pipeline_selector(
                graphene_info, pipeline_selector
            )
        else:
            return get_pipeline_snapshot_or_error_from_snapshot_id(graphene_info, snapshot_id_arg)

    def resolve_version(self, graphene_info):
        return graphene_info.context.version

    def resolve_scheduler(self, graphene_info):
        return get_scheduler_or_error(graphene_info)

    def resolve_scheduleOrError(self, graphene_info, schedule_selector):
        return get_schedule_or_error(
            graphene_info, DagsterScheduleSelector.from_graphql_input(schedule_selector)
        )

    def resolve_schedulesOrError(self, graphene_info, **kwargs):
        return get_schedules_or_error(
            graphene_info,
            DagsterRepositorySelector.from_graphql_input(kwargs.get("repositorySelector")),
        )

    def resolve_sensorOrError(self, graphene_info, sensorSelector):
        return get_sensor_or_error(
            graphene_info, DagsterSensorSelector.from_graphql_input(sensorSelector)
        )

    def resolve_sensorsOrError(self, graphene_info, **kwargs):
        return get_sensors_or_error(
            graphene_info,
            DagsterRepositorySelector.from_graphql_input(kwargs.get("repositorySelector")),
        )

    def resolve_unloadableJobStatesOrError(self, graphene_info, **kwargs):
        job_type = JobType(kwargs["jobType"]) if "jobType" in kwargs else None
        return get_unloadable_job_states_or_error(graphene_info, job_type)

    def resolve_pipelineOrError(self, graphene_info, **kwargs):
        return get_pipeline_or_error(
            graphene_info, pipeline_selector_from_graphql(graphene_info.context, kwargs["params"]),
        )

    def resolve_pipelineRunsOrError(self, _graphene_info, **kwargs):
        filters = kwargs.get("filter")
        if filters is not None:
            filters = filters.to_selector()

        return PipelineRuns(
            filters=filters, cursor=kwargs.get("cursor"), limit=kwargs.get("limit"),
        )

    def resolve_pipelineRunOrError(self, graphene_info, runId):
        return get_run_by_id(graphene_info, runId)

    def resolve_runGroupsOrError(self, graphene_info, **kwargs):
        filters = kwargs.get("filter")
        if filters is not None:
            filters = filters.to_selector()

        return RunGroupsOrError(
            results=get_run_groups(
                graphene_info, filters, kwargs.get("cursor"), kwargs.get("limit")
            )
        )

    def resolve_partitionSetsOrError(self, graphene_info, **kwargs):
        return get_partition_sets_or_error(
            graphene_info,
            DagsterRepositorySelector.from_graphql_input(kwargs.get("repositorySelector")),
            kwargs.get("pipelineName"),
        )

    def resolve_partitionSetOrError(self, graphene_info, **kwargs):
        return get_partition_set(
            graphene_info,
            DagsterRepositorySelector.from_graphql_input(kwargs.get("repositorySelector")),
            kwargs.get("partitionSetName"),
        )

    def resolve_pipelineRunTags(self, graphene_info):
        return get_run_tags(graphene_info)

    def resolve_runGroupOrError(self, graphene_info, runId):
        return get_run_group(graphene_info, runId)

    def resolve_isPipelineConfigValid(self, graphene_info, pipeline, **kwargs):
        return validate_pipeline_config(
            graphene_info,
            pipeline_selector_from_graphql(graphene_info.context, pipeline),
            kwargs.get("runConfigData"),
            kwargs.get("mode"),
        )

    def resolve_executionPlanOrError(self, graphene_info, pipeline, **kwargs):
        return get_execution_plan(
            graphene_info,
            pipeline_selector_from_graphql(graphene_info.context, pipeline),
            kwargs.get("runConfigData"),
            kwargs.get("mode"),
        )

    def resolve_runConfigSchemaOrError(self, graphene_info, **kwargs):
        return resolve_run_config_schema_or_error(
            graphene_info,
            pipeline_selector_from_graphql(graphene_info.context, kwargs["selector"]),
            kwargs.get("mode"),
        )

    def resolve_instance(self, graphene_info):
        return Instance(graphene_info.context.instance)

    def resolve_assetsOrError(self, graphene_info, **kwargs):
        return get_assets(graphene_info, kwargs.get("prefixPath"))

    def resolve_assetOrError(self, graphene_info, **kwargs):
        return get_asset(graphene_info, AssetKey.from_graphql_input(kwargs["assetKey"]))
