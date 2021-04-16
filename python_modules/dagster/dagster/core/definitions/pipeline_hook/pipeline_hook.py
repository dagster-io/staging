from collections import namedtuple
from functools import update_wrapper
from typing import TYPE_CHECKING

from dagster import check
from dagster.core.definitions.sensor import SensorDefinition
from dagster.core.errors import PipelineHookExecutionError, user_code_error_boundary
from dagster.core.storage.pipeline_run import PipelineRunsFilter
from dagster.utils.error import serializable_error_info_from_exc_info

if TYPE_CHECKING:
    from dagster.core.definitions.pipeline import PipelineDefinition


HOOK_SENSOR_PREFIX = "SYSTEM_HOOK_SENSOR_"
# Hook Definition
class PipelineHookDefinition(
    namedtuple("_PipelineHookDefinition", "fn dagster_event_type_value required_resource_keys")
):
    def __new__(cls, fn, dagster_event_type_value, required_resource_keys=None):
        return super(PipelineHookDefinition, cls).__new__(
            cls,
            check.callable_param(fn, "fn"),
            check.str_param(dagster_event_type_value, "dagster_event_type_value"),
            check.opt_set_param(required_resource_keys, "required_resource_keys", str),
        )


class _PipelineHookDecoratorCallable:
    def __init__(self, dagster_event_type_value, required_resource_keys):
        self.dagster_event_type_value = dagster_event_type_value
        self.required_resource_keys = required_resource_keys

    def __call__(self, fn):

        check.callable_param(fn, "fn")

        pipeline_hook_def = PipelineHookDefinition(
            fn, self.dagster_event_type_value, self.required_resource_keys
        )
        update_wrapper(pipeline_hook_def, fn)

        return pipeline_hook_def


def pipeline_hook(dagster_event_type_value, required_resource_keys):
    return _PipelineHookDecoratorCallable(
        dagster_event_type_value, required_resource_keys=required_resource_keys
    )


class PipelineHookContext:
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


# Hook Registration


# class PipelineHookRegistrationData(
#     namedtuple("_PipelineHookRegistrationData", "pipeline_hook_def pipeline_def")
# ):
#     def __new__(cls, pipeline_hook_def, pipeline_def):
#         from dagster.core.definitions.pipeline import PipelineDefinition

#         return super(PipelineHookRegistrationData, cls).__new__(
#             cls,
#             check.inst_param(pipeline_hook_def, "pipeline_hook_def", PipelineHookDefinition),
#             check.inst_param(pipeline_def, "pipeline_def", PipelineDefinition),
#         )

#     def get_sensor_def(self) -> SensorDefinition:
#         return _register_pipeline_hook(self.pipeline_hook_def, self.pipeline_def)


# Hook Invocation?


def register_pipeline_hook(
    pipeline_hook_def: PipelineHookDefinition, pipeline_def: "PipelineDefinition"
) -> SensorDefinition:
    """Convert hook_fn to a dummy pipeline and a sensor

    Args:
        pipeline_hook_def
        pipeline_def
    #     hook_fn (Callable): user code
    #     dagster_event_type_value (DagsterEventType):
    #     required_resource_keys:
    #     resource_config: ??? TODO: find a way to thread it from run_config.hook?
    #     pipeline_run_filters (Optional[PipelineRunsFilter]): db filters

    Returns:
        SensorDefinition
    """
    from dagster.core.definitions.pipeline import PipelineDefinition
    from dagster import sensor
    from dagster.core.execution.build_resources import build_resources
    from dagster.core.events import EngineEventData

    check.inst_param(pipeline_hook_def, "pipeline_hook_def", PipelineHookDefinition)
    check.inst_param(pipeline_def, "pipeline_def", PipelineDefinition)

    dummy_sensor_name = (
        f"{HOOK_SENSOR_PREFIX}{pipeline_def.name}_{pipeline_hook_def.dagster_event_type_value}"
    )

    @sensor(name=dummy_sensor_name, pipeline_name="")
    def _pipeline_hook_dummy_sensor(context):
        # TODO: query: do we alert on historical runs?
        # get runs
        runs = context.instance.get_runs(
            filters=PipelineRunsFilter(
                pipeline_name=pipeline_def.name,
            ),
            # TODO: cursor?
            limit=5,
        )

        for pipeline_run in runs:
            events = context.instance.event_log_storage.get_events_by_type(
                pipeline_run.run_id, pipeline_hook_def.dagster_event_type_value
            )

            success_msg = f'Finished the execution of hook "{pipeline_hook_def.__name__}" triggered by {pipeline_hook_def.dagster_event_type_value}.'

            # TODO: find a better way to way skip the same run
            logs = context.instance.all_logs(pipeline_run.run_id)
            engine_events = [
                evt
                for evt in logs
                if evt.is_dagster_event
                and evt.dagster_event.is_engine_event
                and success_msg in evt.message
            ]
            if engine_events:
                continue

            for event in events:
                mode_def = pipeline_def.get_mode_definition(pipeline_run.mode)
                resource_defs = {
                    resource_key: mode_def.resource_defs[resource_key]
                    for resource_key in pipeline_hook_def.required_resource_keys
                }

                with build_resources(
                    resources=resource_defs,
                    instance=context.instance,
                    resource_config=pipeline_run.run_config.get("resources", {}),
                    pipeline_run=pipeline_run,
                ) as resources:
                    try:
                        with user_code_error_boundary(
                            PipelineHookExecutionError,
                            lambda: f'Error occurred during a hook execution triggered for pipeline "{pipeline_def.name}"',
                        ):
                            pipeline_hook_def.fn(
                                PipelineHookContext(
                                    context.instance, pipeline_run, resources, event
                                )
                            )

                            # log to the original pipeline run
                            context.instance.report_engine_event(
                                message=success_msg,
                                pipeline_run=pipeline_run,
                            )

                    except PipelineHookExecutionError as pipeline_hook_execution_error:
                        # log to the original pipeline run
                        context.instance.report_engine_event(
                            message=f"Error occurred during the hook execution {pipeline_hook_def.__name__} triggered by {pipeline_hook_def.dagster_event_type_value}",
                            pipeline_run=pipeline_run,
                            engine_event_data=EngineEventData.engine_error(
                                serializable_error_info_from_exc_info(
                                    pipeline_hook_execution_error.original_exc_info
                                )
                            ),
                        )
                        # yield something?

    return _pipeline_hook_dummy_sensor
