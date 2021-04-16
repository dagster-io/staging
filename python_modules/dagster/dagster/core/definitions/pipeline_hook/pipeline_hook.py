from collections import namedtuple
from typing import TYPE_CHECKING

from dagster import check
from dagster.core.definitions.event_metadata import EventMetadataEntry
from dagster.core.definitions.resource import ResourceDefinition
from dagster.core.errors import PipelineHookExecutionError, user_code_error_boundary
from dagster.utils.error import serializable_error_info_from_exc_info

if TYPE_CHECKING:
    from dagster.core.definitions.sensor import SensorDefinition


MONITOR_SENSOR_PREFIX = "SYSTEM_SENSOR"


class MonitorSensorData(
    namedtuple("_MonitorSensorData", "fn dagster_event_type name resource_defs resource_config")
):
    def __new__(cls, fn, dagster_event_type, name, resource_defs=None, resource_config=None):
        from dagster.core.events import DagsterEventType

        return super(MonitorSensorData, cls).__new__(
            cls,
            check.callable_param(fn, "fn"),
            check.inst_param(dagster_event_type, "dagster_event_type", DagsterEventType),
            check.str_param(name, "name"),
            check.opt_dict_param(
                resource_defs, "resource_defs", key_type=str, value_type=ResourceDefinition
            ),
            resource_config,
        )


class _PipelineHookDecoratorCallable:
    def __init__(self, dagster_event_type, name=None, resource_defs=None, resource_config=None):
        self.name = name
        self.dagster_event_type = dagster_event_type
        self.resource_defs = resource_defs
        self.resource_config = resource_config

    def __call__(self, fn):

        check.callable_param(fn, "fn")

        name = self.name if self.name else fn.__name__

        monitor_sensor_data = MonitorSensorData(
            fn, self.dagster_event_type, name, self.resource_defs, self.resource_config
        )
        monitor_sensor = make_monitor_sensor(monitor_sensor_data)
        # update_wrapper(monitor_sensor, fn)

        return monitor_sensor


def pipeline_failure_sensor(resource_defs):
    from dagster.core.events import DagsterEventType

    return _PipelineHookDecoratorCallable(
        # TODO: allow multiple types: +DagsterEventType.PIPELINE_INIT_FAILURE
        dagster_event_type=DagsterEventType.PIPELINE_FAILURE,
        resource_defs=resource_defs,
    )


class MonitorSensorContext:
    def __init__(self, instance, pipeline_run, resources, event):
        self._instance = instance
        self._pipeline_run = pipeline_run
        self._resources = resources
        self._event = event

    @property
    def pipeline_run(self):
        return self._pipeline_run

    @property
    def event(self):
        return self._event

    @property
    def resources(self):
        return self._resources

    def log(self, message: str):
        return self._instance.report_engine_event(message, self._pipeline_run)


def make_monitor_sensor(monitor_sensor_data: MonitorSensorData) -> "SensorDefinition":
    """blah.

    Args:
        monitor_sensor_data (MonitorSensorData)

    Returns:
        SensorDefinition
    """
    from dagster import sensor
    from dagster.core.execution.build_resources import build_resources
    from dagster.core.events import EngineEventData, DagsterEventType

    check.inst_param(monitor_sensor_data, "monitor_sensor_data", MonitorSensorData)

    sensor_name = f"{MONITOR_SENSOR_PREFIX}_{monitor_sensor_data.name}"

    @sensor(
        name=sensor_name,
        # TODO: allow 0 target
        pipeline_name="",
        minimum_interval_seconds=10,
    )
    def _monitor_sensor(context):
        # TBD: query: do we alert on historical runs?
        runs = context.instance.get_runs(
            # TODO: skip evaluation
            # https://dagster.phacility.com/D7613
            limit=5,
        )

        for pipeline_run in runs:
            events = context.instance.all_logs(
                pipeline_run.run_id, monitor_sensor_data.dagster_event_type
            )

            success_msg = f'Finished the execution of "{sensor_name}".'

            # TODO: skip evaluation
            logs = context.instance.all_logs(pipeline_run.run_id, DagsterEventType.ENGINE_EVENT)
            engine_events = [evt for evt in logs if success_msg in evt.message]
            if engine_events:
                continue

            for event in events:
                # TBD: no resources? and force users to create slack client inside fn
                with build_resources(
                    resources=monitor_sensor_data.resource_defs,
                    instance=context.instance,
                    resource_config=monitor_sensor_data.resource_config,
                ) as resources:
                    try:
                        with user_code_error_boundary(
                            PipelineHookExecutionError,
                            lambda: f'Error occurred during the execution "{sensor_name}".',
                        ):
                            monitor_sensor_data.fn(
                                MonitorSensorContext(
                                    context.instance, pipeline_run, resources, event
                                )
                            )

                            # log to the original pipeline run
                            context.instance.report_engine_event(
                                message=success_msg,
                                pipeline_run=pipeline_run,
                                engine_event_data=EngineEventData(
                                    [EventMetadataEntry.text(sensor_name, "sensor_name")]
                                ),
                            )
                            # TODO: yield something?

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
                        # TODO: yield something?

    return _monitor_sensor
