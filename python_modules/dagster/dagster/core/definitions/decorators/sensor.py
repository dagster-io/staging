from dagster import check
from dagster.core.definitions.sensor import SensorDefinition
from dagster.utils.backcompat import experimental


@experimental
def sensor(
    pipeline_name,
    run_config_fn=None,
    tags_fn=None,
    sensor_name=None,
    mode="default",
    solid_selection=None,
    environment_vars=None,
):
    """
    The decorated function will be called as the ``should_execute_fn`` of the underlying
    :py:class:`~dagster.ScheduleDefinition` and should take a :py:class:`~dagster.ScheduleContext`
    as its only argument, returning a boolean if the execution should fire

    Args:
        execution (str): The execution to execute when the sensor fires.
        name (str): The name of this sensor
    """
    check.str_param(pipeline_name, "pipeline_name")
    check.opt_callable_param(run_config_fn, 'run_config_fn')
    check.opt_callable_param(tags_fn, 'tags_fn')
    check.opt_str_param(sensor_name, "sensor_name")
    check.opt_str_param(mode, 'mode')
    check.opt_nullable_list_param(solid_selection, 'solid_selection', of_type=str)
    check.opt_dict_param(environment_vars, "environment_vars", key_type=str, value_type=str)

    def inner(fn):
        check.callable_param(fn, "fn")
        name = sensor_name or fn.__name__

        return SensorDefinition(
            name=name,
            pipeline_name=pipeline_name,
            run_config_fn=run_config_fn,
            tags_fn=tags_fn,
            should_execute_fn=fn,
            mode=mode,
            solid_selection=solid_selection,
            environment_vars=environment_vars,
        )

    return inner
