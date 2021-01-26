import graphene

from ...implementation.execution import (
    create_and_launch_partition_backfill,
    delete_pipeline_run,
    launch_pipeline_execution,
    launch_pipeline_reexecution,
    terminate_pipeline_execution,
)
from ...implementation.external import get_full_external_pipeline_or_raise
from ...implementation.utils import ExecutionMetadata
from ...implementation.utils import ExecutionParams as ImplExecutionParams
from ...implementation.utils import (
    UserFacingGraphQLError,
    capture_error,
    pipeline_selector_from_graphql,
)
from ..backfill import PartitionBackfillResult
from ..errors import (
    ConflictingExecutionParamsError,
    PipelineRunNotFoundError,
    PresetNotFoundError,
    PythonError,
    ReloadNotSupported,
    RepositoryLocationNotFound,
)
from ..external import RepositoryLocation, RepositoryLocationLoadFailure
from ..inputs import ExecutionParams, PartitionBackfillParams
from ..pipelines.pipeline import PipelineRun
from ..runs import LaunchPipelineExecutionResult, LaunchPipelineReexecutionResult
from ..schedules import (
    ReconcileSchedulerStateMutation,
    StartScheduleMutation,
    StopRunningScheduleMutation,
)
from ..sensors import StartSensorMutation, StopSensorMutation


def create_execution_params(graphene_info, graphql_execution_params):
    preset_name = graphql_execution_params.get("preset")
    selector = pipeline_selector_from_graphql(
        graphene_info.context, graphql_execution_params["selector"]
    )
    if preset_name:
        if graphql_execution_params.get("runConfigData"):
            raise UserFacingGraphQLError(
                ConflictingExecutionParamsError(conflicting_param="runConfigData")
            )

        if graphql_execution_params.get("mode"):
            raise UserFacingGraphQLError(ConflictingExecutionParamsError(conflicting_param="mode"))

        if selector.solid_selection:
            raise UserFacingGraphQLError(
                ConflictingExecutionParamsError(conflicting_param="selector.solid_selection")
            )

        external_pipeline = get_full_external_pipeline_or_raise(graphene_info, selector)

        if not external_pipeline.has_preset(preset_name):
            raise UserFacingGraphQLError(PresetNotFoundError(preset=preset_name, selector=selector))

        preset = external_pipeline.get_preset(preset_name)

        return ImplExecutionParams(
            selector=selector.with_solid_selection(preset.solid_selection),
            run_config=preset.run_config,
            mode=preset.mode,
            execution_metadata=create_execution_metadata(
                graphql_execution_params.get("executionMetadata")
            ),
            step_keys=graphql_execution_params.get("stepKeys"),
        )

    return execution_params_from_graphql(graphene_info.context, graphql_execution_params)


def execution_params_from_graphql(context, graphql_execution_params):
    return ImplExecutionParams(
        selector=pipeline_selector_from_graphql(context, graphql_execution_params.get("selector")),
        run_config=graphql_execution_params.get("runConfigData") or {},
        mode=graphql_execution_params.get("mode"),
        execution_metadata=create_execution_metadata(
            graphql_execution_params.get("executionMetadata")
        ),
        step_keys=graphql_execution_params.get("stepKeys"),
    )


def create_execution_metadata(graphql_execution_metadata):
    return (
        ExecutionMetadata(
            run_id=graphql_execution_metadata.get("runId"),
            tags={t["key"]: t["value"] for t in graphql_execution_metadata.get("tags", [])},
            root_run_id=graphql_execution_metadata.get("rootRunId"),
            parent_run_id=graphql_execution_metadata.get("parentRunId"),
        )
        if graphql_execution_metadata
        else ExecutionMetadata(run_id=None, tags={})
    )


class DeletePipelineRunSuccess(graphene.ObjectType):
    runId = graphene.NonNull(graphene.String)


class DeletePipelineRunResult(graphene.Union):
    class Meta:
        types = (DeletePipelineRunSuccess, PythonError, PipelineRunNotFoundError)


class DeleteRunMutation(graphene.Mutation):
    class Arguments:
        runId = graphene.NonNull(graphene.String)

    Output = graphene.NonNull(DeletePipelineRunResult)

    def mutate(self, graphene_info, **kwargs):
        run_id = kwargs["runId"]
        return delete_pipeline_run(graphene_info, run_id)


class TerminatePipelineExecutionSuccess(graphene.ObjectType):
    run = graphene.Field(graphene.NonNull(PipelineRun))


class TerminatePipelineExecutionFailure(graphene.ObjectType):
    run = graphene.NonNull(PipelineRun)
    message = graphene.NonNull(graphene.String)


class TerminatePipelineExecutionResult(graphene.Union):
    class Meta:
        types = (
            TerminatePipelineExecutionSuccess,
            TerminatePipelineExecutionFailure,
            PipelineRunNotFoundError,
            PythonError,
        )


@capture_error
def create_execution_params_and_launch_pipeline_exec(graphene_info, execution_params_dict):
    # refactored into a helper function here in order to wrap with @capture_error,
    # because create_execution_params may raise
    return launch_pipeline_execution(
        graphene_info,
        execution_params=create_execution_params(graphene_info, execution_params_dict),
    )


class LaunchPipelineExecutionMutation(graphene.Mutation):
    class Meta:
        description = "Launch a pipeline run via the run launcher configured on the instance."

    class Arguments:
        executionParams = graphene.NonNull(ExecutionParams)

    Output = graphene.NonNull(LaunchPipelineExecutionResult)

    def mutate(self, graphene_info, **kwargs):
        return create_execution_params_and_launch_pipeline_exec(
            graphene_info, kwargs["executionParams"]
        )


class LaunchPartitionBackfillMutation(graphene.Mutation):
    class Meta:
        description = "Launches a set of partition backfill runs via the run launcher configured on the instance."

    class Arguments:
        backfillParams = graphene.NonNull(PartitionBackfillParams)

    Output = graphene.NonNull(PartitionBackfillResult)

    def mutate(self, graphene_info, **kwargs):
        return create_and_launch_partition_backfill(graphene_info, kwargs["backfillParams"])


@capture_error
def create_execution_params_and_launch_pipeline_reexec(graphene_info, execution_params_dict):
    # refactored into a helper function here in order to wrap with @capture_error,
    # because create_execution_params may raise
    return launch_pipeline_reexecution(
        graphene_info,
        execution_params=create_execution_params(graphene_info, execution_params_dict),
    )


class LaunchPipelineReexecutionMutation(graphene.Mutation):
    class Meta:
        description = "Re-launch a pipeline run via the run launcher configured on the instance"

    class Arguments:
        executionParams = graphene.NonNull(ExecutionParams)

    Output = graphene.NonNull(LaunchPipelineReexecutionResult)

    def mutate(self, graphene_info, **kwargs):
        return create_execution_params_and_launch_pipeline_reexec(
            graphene_info, execution_params_dict=kwargs["executionParams"],
        )


class TerminatePipelinePolicy(graphene.Enum):
    # Default behavior: Only mark as canceled if the termination is successful, and after all
    # resources peforming the execution have been shut down.
    SAFE_TERMINATE = "SAFE_TERMINATE"

    # Immediately mark the pipelie as canceled, whether or not the termination was successful.
    # No guarantee that the execution has actually stopped.
    MARK_AS_CANCELED_IMMEDIATELY = "MARK_AS_CANCELED_IMMEDIATELY"


class TerminatePipelineExecutionMutation(graphene.Mutation):
    class Arguments:
        runId = graphene.NonNull(graphene.String)
        terminatePolicy = graphene.Argument(TerminatePipelinePolicy)

    Output = graphene.NonNull(TerminatePipelineExecutionResult)

    def mutate(self, graphene_info, **kwargs):
        return terminate_pipeline_execution(
            graphene_info,
            kwargs["runId"],
            kwargs.get("terminatePolicy", TerminatePipelinePolicy.SAFE_TERMINATE),
        )


class ReloadRepositoryLocationMutationResult(graphene.Union):
    class Meta:
        types = (
            RepositoryLocation,
            ReloadNotSupported,
            RepositoryLocationNotFound,
            RepositoryLocationLoadFailure,
        )


class ReloadRepositoryLocationMutation(graphene.Mutation):
    class Arguments:
        repositoryLocationName = graphene.NonNull(graphene.String)

    Output = graphene.NonNull(ReloadRepositoryLocationMutationResult)

    def mutate(self, graphene_info, **kwargs):
        location_name = kwargs["repositoryLocationName"]

        if not graphene_info.context.has_repository_location(
            location_name
        ) and not graphene_info.context.has_repository_location_error(location_name):
            return RepositoryLocationNotFound(location_name)

        if not graphene_info.context.is_reload_supported(location_name):
            return ReloadNotSupported(location_name)

        graphene_info.context.reload_repository_location(location_name)

        if graphene_info.context.has_repository_location(location_name):
            return RepositoryLocation(graphene_info.context.get_repository_location(location_name))
        else:
            return RepositoryLocationLoadFailure(
                location_name, graphene_info.context.get_repository_location_error(location_name)
            )


class Mutation(graphene.ObjectType):
    launch_pipeline_execution = LaunchPipelineExecutionMutation.Field()
    launch_pipeline_reexecution = LaunchPipelineReexecutionMutation.Field()
    reconcile_scheduler_state = ReconcileSchedulerStateMutation.Field()
    start_schedule = StartScheduleMutation.Field()
    stop_running_schedule = StopRunningScheduleMutation.Field()
    start_sensor = StartSensorMutation.Field()
    stop_sensor = StopSensorMutation.Field()
    terminate_pipeline_execution = TerminatePipelineExecutionMutation.Field()
    delete_pipeline_run = DeleteRunMutation.Field()
    reload_repository_location = ReloadRepositoryLocationMutation.Field()
    launch_partition_backfill = LaunchPartitionBackfillMutation.Field()
