from dagster import check
from dagster.core.definitions.sensor import SensorDefinition
from dagster.utils.backcompat import experimental


@experimental
def sensor(
    pipeline_name, name=None, solid_selection=None, mode=None,
):
    """
    The decorated function will be called to determine whether the provided job should execute,
    taking a :py:class:`~dagster.core.definitions.sensor.SensorExecutionContext`
    as its only argument, returning a boolean if the execution should fire

    Args:
        job_definition (str): The job to execute when the sensor fires.
        name (str): The name of this sensor
    """
    check.opt_str_param(name, "name")

    def inner(fn):
        check.callable_param(fn, "fn")
        sensor_name = name or fn.__name__

        return SensorDefinition(
            name=sensor_name,
            pipeline_name=pipeline_name,
            job_param_fn=fn,
            solid_selection=solid_selection,
            mode=mode,
        )

    return inner
