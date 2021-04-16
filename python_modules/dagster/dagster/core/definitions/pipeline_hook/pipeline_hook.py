from collections import namedtuple
from functools import update_wrapper

from dagster import check
from dagster.builtins import Any
from dagster.core.definitions.pipeline import PipelineDefinition
from dagster.core.definitions.sensor import SensorDefinition
from dagster.core.storage.pipeline_run import PipelineRunsFilter


class _PipelineHook:
    def __init__(self, dagster_event_type_value, pipeline_name, resource_defs):
        self.dagster_event_type_value = dagster_event_type_value
        self.pipeline_name = pipeline_name
        self.resource_defs = resource_defs  # TODO: this is not ok

    def __call__(self, fn):

        check.callable_param(fn, "fn")

        # expected_positionals = ["context"]

        resolved_hook = _make_pipeline_hook(
            fn, self.dagster_event_type_value, self.pipeline_name, self.resource_defs
        )
        update_wrapper(resolved_hook, fn)

        return resolved_hook


def pipeline_hook(dagster_event_type_value, pipeline_name, resource_defs=None):
    return _PipelineHook(dagster_event_type_value, pipeline_name, resource_defs=resource_defs)


class PipelineHookContext:
    def __init__(self, context):
        self._solid_context = context

    @property
    def pipeline_run(self):
        return self._solid_context.solid_config["pipeline_run"]

    @property
    def event(self):
        return self._solid_context.solid_config["event"]

    @property
    def resources(self):
        return self._solid_context.resources

    @property
    def log(self):
        return self._solid_context.log


class PipelineHookData(namedtuple("_PipelineHookData", "pipeline_def sensor_def")):
    def __new__(cls, pipeline_def, sensor_def):
        return super(PipelineHookData, cls).__new__(
            cls,
            check.inst_param(pipeline_def, "pipeline_def", PipelineDefinition),
            check.inst_param(sensor_def, "sensor_def", SensorDefinition),
        )

    def get_defs(self):
        return [self.pipeline_def, self.sensor_def]


def _make_pipeline_hook(hook_fn, dagster_event_type_value, pipeline_name, resource_defs):
    """Convert hook_fn to a dummy pipeline and a sensor

    Args:
        hook_fn (Callable): user code
        dagster_event_type_value:
        resource_defs: TODO: not good
        pipeline_run_filters (Optional[PipelineRunsFilter]): db filters

    Returns:
        PipelineHookData
    """
    from dagster import ModeDefinition, pipeline, sensor, solid, RunRequest

    dummy_pipeline_name = f"dummy_pipeline_for_{pipeline_name}_hook_on_{dagster_event_type_value}"
    dummy_solid_name = f"dummy_solid_for_{dummy_pipeline_name}"
    dummy_sensor_name = f"dummy_sensor_for_{dummy_pipeline_name}"

    required_resource_keys = set(resource_defs.keys())

    @solid(
        name=dummy_solid_name,
        config_schema={"pipeline_run": Any, "event": Any},
        required_resource_keys=required_resource_keys,
    )
    def hook_solid(context):
        hook_fn(PipelineHookContext(context))

    @pipeline(
        name=dummy_pipeline_name,
        mode_defs=[ModeDefinition(resource_defs=resource_defs)],
        tags={"is_pipeline_hook": "true"},
    )
    def pipeline_hook_dummy_pipeline():
        hook_solid()

    @sensor(name=dummy_sensor_name, pipeline_name=dummy_pipeline_name)
    def pipeline_hook_dummy_sensor(context):
        runs = context.instance.get_runs(
            filters=PipelineRunsFilter(
                pipeline_name=pipeline_name,
            ),
            # TODO: cursor?
            limit=5,
        )

        for run in runs:
            events = context.instance.event_log_storage.get_events_by_type(
                run.run_id, dagster_event_type_value
            )
            for event in events:
                yield RunRequest(
                    # TODO: whats unique? rn one pipeline run cooresponds to one hook
                    run_key=str(run.run_id),
                    run_config={
                        "solids": {
                            dummy_solid_name: {"config": {"pipeline_run": run, "event": event}}
                        }
                    },
                )

    return PipelineHookData(
        pipeline_def=pipeline_hook_dummy_pipeline,
        sensor_def=pipeline_hook_dummy_sensor,
    )