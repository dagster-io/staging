from collections import namedtuple
from functools import update_wrapper

from dagster import check
from dagster.core.definitions.pipeline import PipelineDefinition
from dagster.core.definitions.sensor import SensorDefinition


class _PipelineHook:
    def __init__(self, filters, resource_defs):
        self.filters = filters
        self.resource_defs = resource_defs  # TODO: this is not ok

    def __call__(self, fn):

        check.callable_param(fn, "fn")

        # expected_positionals = ["context"]

        resolved_hook = _make_pipeline_hook(fn, self.filters, self.resource_defs)
        update_wrapper(resolved_hook, fn)

        return resolved_hook


def pipeline_hook(filters, resource_defs=None):
    return _PipelineHook(filters=filters, resource_defs=resource_defs)


class PipelineHookContext:
    def __init__(self, context):
        self._solid_context = context

    @property
    def run_id(self):
        return self._solid_context.solid_config["run_id"]

    @property
    def pipeline_name(self):
        return self._solid_context.solid_config["pipeline_name"]

    @property
    def mode(self):
        return self._solid_context.solid_config["mode"]

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


def _make_pipeline_hook(hook_fn, filters, resource_defs):
    """Convert hook_fn to a dummy pipeline and a sensor

    Args:
        hook_fn (Callable): user code
        filters (PipelineRunsFilter): db filters
        resource_defs

    Returns:
        PipelineHookData
    """
    from dagster import ModeDefinition, pipeline, sensor, solid, RunRequest

    required_resource_keys = set(resource_defs.keys())

    @solid(
        config_schema={"run_id": str, "pipeline_name": str, "mode": str},
        required_resource_keys=required_resource_keys,
    )
    def hook_solid(context):
        hook_fn(PipelineHookContext(context))

    @pipeline(
        mode_defs=[ModeDefinition(resource_defs=resource_defs)],
        tags={"is_pipeline_hook": "true"},
    )
    def pipeline_kicked_off_by_sensor_acting_as_pipeline_hook():
        hook_solid()

    @sensor(pipeline_name="pipeline_kicked_off_by_sensor_acting_as_pipeline_hook")
    def pipeline_hook_sensor(context):
        # TODO: make it event based???????
        runs = context.instance.get_runs(
            filters=filters,
            limit=5,
        )

        for run in runs:

            yield RunRequest(
                # TODO: whats unique? rn one pipeline run cooresponds to one hook
                run_key=str(run.run_id),
                run_config={
                    "solids": {
                        "hook_solid": {
                            "config": {
                                "pipeline_name": run.pipeline_name,
                                "run_id": run.run_id,
                                "mode": run.mode,
                            }
                        }
                    }
                },
            )

    return PipelineHookData(
        pipeline_def=pipeline_kicked_off_by_sensor_acting_as_pipeline_hook,
        sensor_def=pipeline_hook_sensor,
    )


# Defining pipeline hooks
# class PipelineHookDefinition(
#     namedtuple("_PipelineHookDefinition", "hook_fn rpipeline_name resource_defs")
# ):
#     def __new__(cls, hook_fn, pipeline_name, resource_defs):
#         return super(PipelineHookDefinition, cls).__new__(
#             cls, hook_fn, pipeline_name, resource_defs
#         )

#     @property
#     def required_resource_keys(self):
#         self.resource_defs.keys()
