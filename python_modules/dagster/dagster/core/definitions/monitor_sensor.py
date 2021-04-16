from typing import Callable, Optional, Union

from dagster import check
from dagster.core.definitions.event_metadata import EventMetadataEntry
from dagster.core.definitions.sensor import (
    MONITOR_SENSOR_PREFIX,
    SensorDefinition,
    SensorExecutionContext,
    SkipReason,
)
from dagster.core.errors import PipelineHookExecutionError, user_code_error_boundary
from dagster.core.events import DagsterEventType, EngineEventData
from dagster.utils.error import serializable_error_info_from_exc_info


class MonitorSensorContext:
    def __init__(self, sensor_name, instance, pipeline_run, events):
        self._sensor_name = sensor_name
        self._instance = instance
        self._pipeline_run = pipeline_run
        self._events = events

    @property
    def pipeline_run(self):
        return self._pipeline_run

    @property
    def events(self):
        return self._events

    def log(self, message: str):
        return self._instance.report_engine_event(
            message,
            self._pipeline_run,
            engine_event_data=EngineEventData([EventMetadataEntry.text(self._sensor_name, "from")]),
        )


def pipeline_failure_monitor(
    name: Optional[str] = None,
    minimum_interval_seconds: Optional[int] = None,
    description: Optional[str] = None,
) -> Callable[[Callable[[MonitorSensorContext], Union[SkipReason]]], SensorDefinition]:
    """
    Creates a pipeline failure monitor where the decorated function is used as the sensor's evaluation function.  The
    decorated function may:

    2. TODO: Yield multiple of `MonitorRequest` objects.
    3. Return or yield a `SkipReason` object, providing a descriptive message of why no runs were
       requested.
    4. Return or yield nothing (skipping without providing a reason)

    Takes a :py:class:`~dagster.MonitorSensorContext`.

    Args:
        name (Optional[str]): The name of the monitor. Defaults to the name of the decorated
            function.
        minimum_interval_seconds (Optional[int]): The minimum number of seconds that will elapse
            between sensor evaluations.
        description (Optional[str]): A human-readable description of the sensor.
    """

    # TODO: allow multiple types: +DagsterEventType.PIPELINE_INIT_FAILURE
    dagster_event_type = DagsterEventType.PIPELINE_FAILURE

    def inner(fn: Callable[["MonitorSensorContext"], Union[SkipReason]]) -> SensorDefinition:
        check.callable_param(fn, "fn")
        sensor_name = f"{MONITOR_SENSOR_PREFIX}_{name or fn.__name__}"

        def _wrapped_fn(context: SensorExecutionContext):
            # depends on https://dagster.phacility.com/D7613
            # avoid unnecessary evaluation evaluation
            # when the daemon is down, bc we persist the cursor info, we can go back to where we
            # left and backfill alerts for the qualified runs during the downtime
            runs = context.instance.get_runs(after_cursor=context.cursor, limit=5)
            if len(runs) == 0:
                yield SkipReason(f"No qualified runs found (after cursor={context.cursor})")
                return

            for pipeline_run in runs:
                events = context.instance.all_logs(pipeline_run.run_id, dagster_event_type)

                if len(events) == 0:
                    context.update_cursor(pipeline_run.run_id)
                    continue

                try:
                    with user_code_error_boundary(
                        PipelineHookExecutionError,
                        lambda: f'Error occurred during the execution "{sensor_name}".',
                    ):
                        # one user code invocation maps to N qualified events
                        fn(
                            MonitorSensorContext(
                                sensor_name, context.instance, pipeline_run, events
                            )
                        )

                        # log to the original pipeline run
                        context.instance.report_engine_event(
                            message=f'Finished the execution of "{sensor_name}".',
                            pipeline_run=pipeline_run,
                            engine_event_data=EngineEventData(
                                [EventMetadataEntry.text(sensor_name, "from")]
                            ),
                        )
                        # TODO: yield something to indicate the monitor success instead of "skipping"

                        # update cursor
                        context.update_cursor(pipeline_run.run_id)

                except PipelineHookExecutionError as pipeline_hook_execution_error:
                    # log to the original pipeline run
                    context.instance.report_engine_event(
                        message=f'Error occurred during the execution of "{sensor_name}".',
                        pipeline_run=pipeline_run,
                        engine_event_data=EngineEventData.engine_error(
                            serializable_error_info_from_exc_info(
                                pipeline_hook_execution_error.original_exc_info
                            )
                        ),
                    )
                    # TODO: yield something to indicate the monitor error instead of "skipping"

        return SensorDefinition(
            name=sensor_name,
            evaluation_fn=_wrapped_fn,
            minimum_interval_seconds=minimum_interval_seconds,
            description=description,
        )

    return inner
