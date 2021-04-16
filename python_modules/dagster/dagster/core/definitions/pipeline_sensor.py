from datetime import datetime
from typing import Callable, Optional, Union

import pendulum
from dagster import check
from dagster.core.definitions.event_metadata import EventMetadataEntry
from dagster.core.definitions.sensor import (
    MonitorRequest,
    SensorDefinition,
    SensorExecutionContext,
    SkipReason,
)
from dagster.core.errors import PipelineSensorExecutionError, user_code_error_boundary
from dagster.core.events import DagsterEventType, EngineEventData
from dagster.core.storage.pipeline_run import PipelineRunStatus, PipelineRunsFilter
from dagster.utils.error import serializable_error_info_from_exc_info


class PipelineSensorContext:
    def __init__(self, sensor_name, pipeline_run, events):
        self._sensor_name = sensor_name
        self._pipeline_run = pipeline_run
        self._events = events

    @property
    def pipeline_run(self):
        return self._pipeline_run

    @property
    def events(self):
        return self._events


def pipeline_failure_monitor(
    name: Optional[str] = None,
    minimum_interval_seconds: Optional[int] = None,
    description: Optional[str] = None,
) -> Callable[
    [Callable[[PipelineSensorContext], Union[SkipReason, MonitorRequest]]], SensorDefinition
]:
    """
    Creates a pipeline failure monitor where the decorated function is used as the sensor's evaluation function.  The
    decorated function may:

    1. Yield multiple of `MonitorRequest` objects, which record the metadata like the ids of the
       originating pipeline runs.
    2. Return or yield a `SkipReason` object, providing a descriptive message of why no runs were
       requested.
    3. Return or yield nothing (skipping without providing a reason)

    Takes a :py:class:`~dagster.PipelineSensorContext`.

    Args:
        name (Optional[str]): The name of the monitor. Defaults to the name of the decorated
            function.
        minimum_interval_seconds (Optional[int]): The minimum number of seconds that will elapse
            between sensor evaluations.
        description (Optional[str]): A human-readable description of the sensor.
    """

    # TODO: allow multiple types: +DagsterEventType.PIPELINE_INIT_FAILURE
    dagster_event_type = DagsterEventType.PIPELINE_FAILURE

    def inner(
        fn: Callable[["PipelineSensorContext"], Union[SkipReason, MonitorRequest]]
    ) -> SensorDefinition:
        check.callable_param(fn, "fn")
        if name is None or callable(name):
            sensor_name = fn.__name__
        else:
            sensor_name = name

        def _wrapped_fn(context: SensorExecutionContext):
            # Initiate the cursor to be the current datetime in UTC first time init (cursor is None)
            if context.cursor is None:
                curr_time = pendulum.now("UTC").isoformat()
                context.update_cursor(curr_time)
                yield SkipReason(
                    f"Initiating {sensor_name}. Set cursor to {datetime.fromisoformat(curr_time)}"
                )
                return

            # Fetch runs where the statuses were updated after the cursor time
            # * we move the cursor forward to the latest visited run's update_timestamp to avoid revisit runs
            # * we filter the query by failure status to reduce the number of scanned rows
            # * when the daemon is down, bc we persist the cursor info, we can go back to where we
            #   left and backfill alerts for the qualified runs (up to 5 at a time) during the downtime
            runs = context.instance.get_runs_by_timestamp(
                filters=PipelineRunsFilter(statuses=[PipelineRunStatus.FAILURE]),
                update_after=datetime.fromisoformat(context.cursor),
                limit=5,
            )

            if len(runs) == 0:
                yield SkipReason(
                    f"No qualified runs found (no runs updated after {datetime.fromisoformat(context.cursor)})"
                )
                return

            for pipeline_run, update_timestamp in runs:
                events = context.instance.all_logs(pipeline_run.run_id, dagster_event_type)

                if len(events) == 0:
                    context.update_cursor(update_timestamp.isoformat())
                    continue

                try:
                    with user_code_error_boundary(
                        PipelineSensorExecutionError,
                        lambda: f'Error occurred during the execution "{sensor_name}".',
                    ):
                        # one user code invocation maps to N qualified events
                        fn(PipelineSensorContext(sensor_name, pipeline_run, events))
                except PipelineSensorExecutionError as pipeline_sensor_execution_error:
                    # log to the original pipeline run
                    context.instance.report_engine_event(
                        message=f'Error occurred during the execution of "{sensor_name}" for run {pipeline_run.run_id}.',
                        pipeline_run=pipeline_run,
                        engine_event_data=EngineEventData.engine_error(
                            serializable_error_info_from_exc_info(
                                pipeline_sensor_execution_error.original_exc_info
                            )
                        ),
                    )

                # log to the original pipeline run
                context.instance.report_engine_event(
                    message=f'Finished the execution of "{sensor_name}" for run {pipeline_run.run_id}.',
                    pipeline_run=pipeline_run,
                    engine_event_data=EngineEventData(
                        [EventMetadataEntry.text(sensor_name, "from")]
                    ),
                )
                context.update_cursor(update_timestamp.isoformat())

                # yield MonitorRequest to indicate the execution success so
                # the sensor machinery would update cursor and job state with the origin_run_id
                yield MonitorRequest(
                    origin_run_id=pipeline_run.run_id,
                    message=f'Finished the execution of "{sensor_name}" for run {pipeline_run.run_id}.',
                )

        return SensorDefinition(
            name=sensor_name,
            evaluation_fn=_wrapped_fn,
            minimum_interval_seconds=minimum_interval_seconds,
            description=description,
            _is_pipeline_sensor=True,
        )

    # This case is for when decorator is used bare, without arguments, i.e. @pipeline_failure_monitor
    if callable(name):
        return inner(name)

    return inner
