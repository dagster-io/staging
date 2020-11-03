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

    The decorated function will be called at an interval to determine whether a run should be
    launched or not. Takes a :py:class:`~dagster.SensorExecutionContext` and returns a
    SensorTickData.

    Args:
        name (str): The name of this sensor
        sensor_tick_fn (Callable[[SensorExecutionContext], SensorTickData]): A function
            that runs at an interval to determine whether a run should be launched or not. Takes a
            :py:class:`~dagster.SensorExecutionContext` and returns a SensorTickData.
        solid_selection (Optional[List[str]]): A list of solid subselection (including single
            solid names) to execute for runs for this sensor e.g.
            ``['*some_solid+', 'other_solid']``
        mode (Optional[str]): The mode to apply when executing runs for this sensor.
            (default: 'default')
    """
    check.opt_str_param(name, "name")

    def inner(fn):
        check.callable_param(fn, "fn")
        sensor_name = name or fn.__name__

        return SensorDefinition(
            name=sensor_name,
            pipeline_name=pipeline_name,
            sensor_tick_fn=fn,
            solid_selection=solid_selection,
            mode=mode,
        )

    return inner
