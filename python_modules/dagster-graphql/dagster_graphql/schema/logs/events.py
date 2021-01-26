import graphene
from dagster import check
from dagster.core.execution.stats import RunStepKeyStatsSnapshot

from ...implementation.fetch_runs import get_step_stats
from ..asset_key import AssetKey
from ..errors import PythonError
from ..runs import StepEventStatus
from ..util import non_null_list
from .log_level import LogLevel


class MessageEvent(graphene.Interface):
    runId = graphene.NonNull(graphene.String)
    message = graphene.NonNull(graphene.String)
    timestamp = graphene.NonNull(graphene.String)
    level = graphene.NonNull(LogLevel)
    stepKey = graphene.Field(graphene.String)
    solidHandleID = graphene.Field(graphene.String)


class EventMetadataEntry(graphene.Interface):
    label = graphene.NonNull(graphene.String)
    description = graphene.String()


class DisplayableEvent(graphene.Interface):
    label = graphene.NonNull(graphene.String)
    description = graphene.String()
    metadataEntries = non_null_list(EventMetadataEntry)


class MissingRunIdErrorEvent(graphene.ObjectType):
    invalidRunId = graphene.NonNull(graphene.String)


class LogMessageEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent,)


class PipelineEvent(graphene.Interface):
    pipelineName = graphene.NonNull(graphene.String)


class PipelineEnqueuedEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, PipelineEvent)


class PipelineDequeuedEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, PipelineEvent)


class PipelineStartingEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, PipelineEvent)


class PipelineCancelingEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, PipelineEvent)


class PipelineCanceledEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, PipelineEvent)


class PipelineStartEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, PipelineEvent)


class PipelineSuccessEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, PipelineEvent)


class PipelineFailureEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, PipelineEvent)

    error = graphene.Field(PythonError)


class PipelineInitFailureEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, PipelineEvent)

    error = graphene.NonNull(PythonError)


class StepEvent(graphene.Interface):
    stepKey = graphene.Field(graphene.String)
    solidHandleID = graphene.Field(graphene.String)


class ExecutionStepStartEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)


class ExecutionStepRestartEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)


class ExecutionStepUpForRetryEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)

    error = graphene.NonNull(PythonError)
    secondsToWait = graphene.Field(graphene.Int)


class ExecutionStepSkippedEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)


class EventPathMetadataEntry(graphene.ObjectType):
    class Meta:
        interfaces = (EventMetadataEntry,)

    path = graphene.NonNull(graphene.String)


class EventJsonMetadataEntry(graphene.ObjectType):
    class Meta:
        interfaces = (EventMetadataEntry,)

    jsonString = graphene.NonNull(graphene.String)


class EventTextMetadataEntry(graphene.ObjectType):
    class Meta:
        interfaces = (EventMetadataEntry,)

    text = graphene.NonNull(graphene.String)


class EventUrlMetadataEntry(graphene.ObjectType):
    class Meta:
        interfaces = (EventMetadataEntry,)

    url = graphene.NonNull(graphene.String)


class EventMarkdownMetadataEntry(graphene.ObjectType):
    class Meta:
        interfaces = (EventMetadataEntry,)

    md_str = graphene.NonNull(graphene.String)


class EventPythonArtifactMetadataEntry(graphene.ObjectType):
    class Meta:
        name = "EventPythonArtifactMetadataEntry"
        interfaces = (EventMetadataEntry,)

    module = graphene.NonNull(graphene.String)
    name = graphene.NonNull(graphene.String)


class EventFloatMetadataEntry(graphene.ObjectType):
    class Meta:
        interfaces = (EventMetadataEntry,)

    floatValue = graphene.NonNull(graphene.Float)


class EventIntMetadataEntry(graphene.ObjectType):
    class Meta:
        interfaces = (EventMetadataEntry,)

    intValue = graphene.NonNull(graphene.Int)


class ObjectStoreOperationType(graphene.Enum):
    SET_OBJECT = "SET_OBJECT"
    GET_OBJECT = "GET_OBJECT"
    RM_OBJECT = "RM_OBJECT"
    CP_OBJECT = "CP_OBJECT"


class ObjectStoreOperationResult(graphene.ObjectType):
    class Meta:
        interfaces = (DisplayableEvent,)

    op = graphene.NonNull(ObjectStoreOperationType)

    def resolve_metadataEntries(self, _graphene_info):
        from ...implementation.events import _to_metadata_entries

        return _to_metadata_entries(self.metadata_entries)  # pylint: disable=no-member


class Materialization(graphene.ObjectType):
    class Meta:
        interfaces = (DisplayableEvent,)

    assetKey = graphene.Field(AssetKey)

    def resolve_metadataEntries(self, _graphene_info):
        from ...implementation.events import _to_metadata_entries

        return _to_metadata_entries(self.metadata_entries)  # pylint: disable=no-member

    def resolve_assetKey(self, _graphene_info):
        asset_key = self.asset_key  # pylint: disable=no-member

        if not asset_key:
            return None

        return AssetKey(path=asset_key.path)


class ExpectationResult(graphene.ObjectType):
    class Meta:
        interfaces = (DisplayableEvent,)

    success = graphene.NonNull(graphene.Boolean)

    def resolve_metadataEntries(self, _graphene_info):
        from ...implementation.events import _to_metadata_entries

        return _to_metadata_entries(self.metadata_entries)  # pylint: disable=no-member


class TypeCheck(graphene.ObjectType):
    class Meta:
        interfaces = (DisplayableEvent,)

    success = graphene.NonNull(graphene.Boolean)

    def resolve_metadataEntries(self, _graphene_info):
        from ...implementation.events import _to_metadata_entries

        return _to_metadata_entries(self.metadata_entries)  # pylint: disable=no-member


class FailureMetadata(graphene.ObjectType):
    class Meta:
        interfaces = (DisplayableEvent,)

    def resolve_metadataEntries(self, _graphene_info):
        from ...implementation.events import _to_metadata_entries

        return _to_metadata_entries(self.metadata_entries)  # pylint: disable=no-member


class ExecutionStepInputEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)

    input_name = graphene.NonNull(graphene.String)
    type_check = graphene.NonNull(TypeCheck)


class ExecutionStepOutputEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)

    output_name = graphene.NonNull(graphene.String)
    type_check = graphene.NonNull(TypeCheck)


class ExecutionStepSuccessEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)


class ExecutionStepFailureEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)

    error = graphene.NonNull(PythonError)
    failureMetadata = graphene.Field(FailureMetadata)


class HookCompletedEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)


class HookSkippedEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)


class HookErroredEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)

    error = graphene.NonNull(PythonError)


class StepMaterializationEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)

    materialization = graphene.NonNull(Materialization)
    stepStats = graphene.NonNull(lambda: PipelineRunStepStats)

    def resolve_stepStats(self, graphene_info):
        run_id = self.runId  # pylint: disable=no-member
        step_key = self.stepKey  # pylint: disable=no-member
        stats = get_step_stats(graphene_info, run_id, step_keys=[step_key])
        return stats[0]


class HandledOutputEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)

    output_name = graphene.NonNull(graphene.String)
    manager_key = graphene.NonNull(graphene.String)


class LoadedInputEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)

    input_name = graphene.NonNull(graphene.String)
    manager_key = graphene.NonNull(graphene.String)
    upstream_output_name = graphene.Field(graphene.String)
    upstream_step_key = graphene.Field(graphene.String)


class ObjectStoreOperationEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)

    operation_result = graphene.NonNull(ObjectStoreOperationResult)


class EngineEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, DisplayableEvent, StepEvent)

    error = graphene.Field(PythonError)
    marker_start = graphene.Field(graphene.String)
    marker_end = graphene.Field(graphene.String)


class StepExpectationResultEvent(graphene.ObjectType):
    class Meta:
        interfaces = (MessageEvent, StepEvent)

    expectation_result = graphene.NonNull(ExpectationResult)


# Should be a union of all possible events
class PipelineRunEvent(graphene.Union):
    class Meta:
        types = (
            ExecutionStepFailureEvent,
            ExecutionStepInputEvent,
            ExecutionStepOutputEvent,
            ExecutionStepSkippedEvent,
            ExecutionStepStartEvent,
            ExecutionStepSuccessEvent,
            ExecutionStepUpForRetryEvent,
            ExecutionStepRestartEvent,
            LogMessageEvent,
            PipelineFailureEvent,
            PipelineInitFailureEvent,
            PipelineStartEvent,
            PipelineEnqueuedEvent,
            PipelineDequeuedEvent,
            PipelineStartingEvent,
            PipelineCancelingEvent,
            PipelineCanceledEvent,
            PipelineSuccessEvent,
            HandledOutputEvent,
            LoadedInputEvent,
            ObjectStoreOperationEvent,
            StepExpectationResultEvent,
            StepMaterializationEvent,
            EngineEvent,
            HookCompletedEvent,
            HookSkippedEvent,
            HookErroredEvent,
        )


class PipelineRunStepStats(graphene.ObjectType):
    runId = graphene.NonNull(graphene.String)
    stepKey = graphene.NonNull(graphene.String)
    status = graphene.Field(StepEventStatus)
    startTime = graphene.Field(graphene.Float)
    endTime = graphene.Field(graphene.Float)
    materializations = non_null_list(Materialization)
    expectationResults = non_null_list(ExpectationResult)

    def __init__(self, stats):
        self._stats = check.inst_param(stats, "stats", RunStepKeyStatsSnapshot)
        super().__init__(
            runId=stats.run_id,
            stepKey=stats.step_key,
            status=stats.status,
            startTime=stats.start_time,
            endTime=stats.end_time,
            materializations=stats.materializations,
            expectationResults=stats.expectation_results,
        )
