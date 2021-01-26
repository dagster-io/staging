from dagster import check, seven
from dagster.core.definitions.events import (
    EventMetadataEntry,
    FloatMetadataEntryData,
    IntMetadataEntryData,
    JsonMetadataEntryData,
    MarkdownMetadataEntryData,
    PathMetadataEntryData,
    PythonArtifactMetadataEntryData,
    TextMetadataEntryData,
    UrlMetadataEntryData,
)
from dagster.core.events import DagsterEventType
from dagster.core.events.log import EventRecord
from dagster.core.execution.plan.objects import StepFailureData


def iterate_metadata_entries(metadata_entries):
    from ..schema.logs.events import (
        EventFloatMetadataEntry,
        EventIntMetadataEntry,
        EventJsonMetadataEntry,
        EventMarkdownMetadataEntry,
        EventPathMetadataEntry,
        EventPythonArtifactMetadataEntry,
        EventTextMetadataEntry,
        EventUrlMetadataEntry,
    )

    check.list_param(metadata_entries, "metadata_entries", of_type=EventMetadataEntry)
    for metadata_entry in metadata_entries:
        if isinstance(metadata_entry.entry_data, PathMetadataEntryData):
            yield EventPathMetadataEntry(
                label=metadata_entry.label,
                description=metadata_entry.description,
                path=metadata_entry.entry_data.path,
            )
        elif isinstance(metadata_entry.entry_data, JsonMetadataEntryData):
            yield EventJsonMetadataEntry(
                label=metadata_entry.label,
                description=metadata_entry.description,
                jsonString=seven.json.dumps(metadata_entry.entry_data.data),
            )
        elif isinstance(metadata_entry.entry_data, TextMetadataEntryData):
            yield EventTextMetadataEntry(
                label=metadata_entry.label,
                description=metadata_entry.description,
                text=metadata_entry.entry_data.text,
            )
        elif isinstance(metadata_entry.entry_data, UrlMetadataEntryData):
            yield EventUrlMetadataEntry(
                label=metadata_entry.label,
                description=metadata_entry.description,
                url=metadata_entry.entry_data.url,
            )
        elif isinstance(metadata_entry.entry_data, MarkdownMetadataEntryData):
            yield EventMarkdownMetadataEntry(
                label=metadata_entry.label,
                description=metadata_entry.description,
                md_str=metadata_entry.entry_data.md_str,
            )
        elif isinstance(metadata_entry.entry_data, PythonArtifactMetadataEntryData):
            yield EventPythonArtifactMetadataEntry(
                label=metadata_entry.label,
                description=metadata_entry.description,
                module=metadata_entry.entry_data.module,
                name=metadata_entry.entry_data.name,
            )
        elif isinstance(metadata_entry.entry_data, FloatMetadataEntryData):
            yield EventFloatMetadataEntry(
                label=metadata_entry.label,
                description=metadata_entry.description,
                floatValue=metadata_entry.entry_data.value,
            )
        elif isinstance(metadata_entry.entry_data, IntMetadataEntryData):
            yield EventIntMetadataEntry(
                label=metadata_entry.label,
                description=metadata_entry.description,
                intValue=metadata_entry.entry_data.value,
            )
        else:
            # skip rest for now
            check.not_implemented(
                "{} unsupported metadata entry for now".format(type(metadata_entry.entry_data))
            )


def _to_metadata_entries(metadata_entries):
    return list(iterate_metadata_entries(metadata_entries) or [])


def from_dagster_event_record(event_record, pipeline_name):
    from ..schema.errors import PythonError
    from ..schema.logs.events import (
        EngineEvent,
        ExecutionStepFailureEvent,
        ExecutionStepInputEvent,
        ExecutionStepOutputEvent,
        ExecutionStepRestartEvent,
        ExecutionStepSkippedEvent,
        ExecutionStepStartEvent,
        ExecutionStepSuccessEvent,
        ExecutionStepUpForRetryEvent,
        HandledOutputEvent,
        HookCompletedEvent,
        HookErroredEvent,
        HookSkippedEvent,
        LoadedInputEvent,
        ObjectStoreOperationEvent,
        PipelineCanceledEvent,
        PipelineCancelingEvent,
        PipelineDequeuedEvent,
        PipelineEnqueuedEvent,
        PipelineFailureEvent,
        PipelineInitFailureEvent,
        PipelineStartEvent,
        PipelineStartingEvent,
        PipelineSuccessEvent,
        StepExpectationResultEvent,
        StepMaterializationEvent,
    )

    # Lots of event types. Pylint thinks there are too many branches
    # pylint: disable=too-many-branches
    check.inst_param(event_record, "event_record", EventRecord)
    check.param_invariant(event_record.is_dagster_event, "event_record")
    check.str_param(pipeline_name, "pipeline_name")

    dagster_event = event_record.dagster_event
    basic_params = construct_basic_params(event_record)
    if dagster_event.event_type == DagsterEventType.STEP_START:
        return ExecutionStepStartEvent(**basic_params)
    elif dagster_event.event_type == DagsterEventType.STEP_SKIPPED:
        return ExecutionStepSkippedEvent(**basic_params)
    elif dagster_event.event_type == DagsterEventType.STEP_UP_FOR_RETRY:
        return ExecutionStepUpForRetryEvent(
            error=dagster_event.step_retry_data.error,
            secondsToWait=dagster_event.step_retry_data.seconds_to_wait,
            **basic_params,
        )
    elif dagster_event.event_type == DagsterEventType.STEP_RESTARTED:
        return ExecutionStepRestartEvent(**basic_params)
    elif dagster_event.event_type == DagsterEventType.STEP_SUCCESS:
        return ExecutionStepSuccessEvent(**basic_params)
    elif dagster_event.event_type == DagsterEventType.STEP_INPUT:
        input_data = dagster_event.event_specific_data
        return ExecutionStepInputEvent(
            input_name=input_data.input_name, type_check=input_data.type_check_data, **basic_params
        )
    elif dagster_event.event_type == DagsterEventType.STEP_OUTPUT:
        output_data = dagster_event.step_output_data
        return ExecutionStepOutputEvent(
            output_name=output_data.output_name,
            type_check=output_data.type_check_data,
            **basic_params,
        )
    elif dagster_event.event_type == DagsterEventType.STEP_MATERIALIZATION:
        materialization = dagster_event.step_materialization_data.materialization
        return StepMaterializationEvent(materialization=materialization, **basic_params)
    elif dagster_event.event_type == DagsterEventType.STEP_EXPECTATION_RESULT:
        expectation_result = dagster_event.event_specific_data.expectation_result
        return StepExpectationResultEvent(expectation_result=expectation_result, **basic_params)
    elif dagster_event.event_type == DagsterEventType.STEP_FAILURE:
        check.inst(dagster_event.step_failure_data, StepFailureData)
        return ExecutionStepFailureEvent(
            error=PythonError(dagster_event.step_failure_data.error),
            failureMetadata=dagster_event.step_failure_data.user_failure_data,
            **basic_params,
        )
    elif dagster_event.event_type == DagsterEventType.PIPELINE_ENQUEUED:
        return PipelineEnqueuedEvent(pipelineName=pipeline_name, **basic_params)
    elif dagster_event.event_type == DagsterEventType.PIPELINE_DEQUEUED:
        return PipelineDequeuedEvent(pipelineName=pipeline_name, **basic_params)
    elif dagster_event.event_type == DagsterEventType.PIPELINE_STARTING:
        return PipelineStartingEvent(pipelineName=pipeline_name, **basic_params)
    elif dagster_event.event_type == DagsterEventType.PIPELINE_CANCELING:
        return PipelineCancelingEvent(pipelineName=pipeline_name, **basic_params)
    elif dagster_event.event_type == DagsterEventType.PIPELINE_CANCELED:
        return PipelineCanceledEvent(pipelineName=pipeline_name, **basic_params)
    elif dagster_event.event_type == DagsterEventType.PIPELINE_START:
        return PipelineStartEvent(pipelineName=pipeline_name, **basic_params)
    elif dagster_event.event_type == DagsterEventType.PIPELINE_SUCCESS:
        return PipelineSuccessEvent(pipelineName=pipeline_name, **basic_params)
    elif dagster_event.event_type == DagsterEventType.PIPELINE_FAILURE:
        return PipelineFailureEvent(
            pipelineName=pipeline_name,
            error=PythonError(dagster_event.pipeline_failure_data.error)
            if (dagster_event.pipeline_failure_data and dagster_event.pipeline_failure_data.error)
            else None,
            **basic_params,
        )

    elif dagster_event.event_type == DagsterEventType.PIPELINE_INIT_FAILURE:
        return PipelineInitFailureEvent(
            pipelineName=pipeline_name,
            error=PythonError(dagster_event.pipeline_init_failure_data.error),
            **basic_params,
        )
    elif dagster_event.event_type == DagsterEventType.HANDLED_OUTPUT:
        return HandledOutputEvent(
            output_name=dagster_event.event_specific_data.output_name,
            manager_key=dagster_event.event_specific_data.manager_key,
            **basic_params,
        )
    elif dagster_event.event_type == DagsterEventType.LOADED_INPUT:
        return LoadedInputEvent(
            input_name=dagster_event.event_specific_data.input_name,
            manager_key=dagster_event.event_specific_data.manager_key,
            upstream_output_name=dagster_event.event_specific_data.upstream_output_name,
            upstream_step_key=dagster_event.event_specific_data.upstream_step_key,
            **basic_params,
        )
    elif dagster_event.event_type == DagsterEventType.OBJECT_STORE_OPERATION:
        operation_result = dagster_event.event_specific_data
        return ObjectStoreOperationEvent(operation_result=operation_result, **basic_params)
    elif dagster_event.event_type == DagsterEventType.ENGINE_EVENT:
        return EngineEvent(
            metadataEntries=_to_metadata_entries(dagster_event.engine_event_data.metadata_entries),
            error=PythonError(dagster_event.engine_event_data.error)
            if dagster_event.engine_event_data.error
            else None,
            marker_start=dagster_event.engine_event_data.marker_start,
            marker_end=dagster_event.engine_event_data.marker_end,
            **basic_params,
        )
    elif dagster_event.event_type == DagsterEventType.HOOK_COMPLETED:
        return HookCompletedEvent(**basic_params)
    elif dagster_event.event_type == DagsterEventType.HOOK_SKIPPED:
        return HookSkippedEvent(**basic_params)
    elif dagster_event.event_type == DagsterEventType.HOOK_ERRORED:
        return HookErroredEvent(
            error=PythonError(dagster_event.hook_errored_data.error), **basic_params
        )
    else:
        raise Exception(
            "Unknown DAGSTER_EVENT type {inner_type} found in logs".format(
                inner_type=dagster_event.event_type
            )
        )


def from_event_record(event_record, pipeline_name):
    from ..schema.logs.events import LogMessageEvent

    check.inst_param(event_record, "event_record", EventRecord)
    check.str_param(pipeline_name, "pipeline_name")

    if event_record.is_dagster_event:
        return from_dagster_event_record(event_record, pipeline_name)
    else:
        return LogMessageEvent(**construct_basic_params(event_record))


def construct_basic_params(event_record):
    from ..schema.logs.log_level import LogLevel

    check.inst_param(event_record, "event_record", EventRecord)
    return {
        "runId": event_record.run_id,
        "message": event_record.dagster_event.message
        if (event_record.dagster_event and event_record.dagster_event.message)
        else event_record.user_message,
        "timestamp": int(event_record.timestamp * 1000),
        "level": LogLevel.from_level(event_record.level),
        "stepKey": event_record.step_key,
        "solidHandleID": event_record.dagster_event.solid_handle.to_string()
        if event_record.is_dagster_event and event_record.dagster_event.solid_handle
        else None,
    }
