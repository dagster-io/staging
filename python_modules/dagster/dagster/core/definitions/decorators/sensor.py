from dagster import check
from dagster.core.definitions.job import JobDefinition
from dagster.core.definitions.sensor import SensorDefinition
from dagster.utils.backcompat import experimental


@experimental
def sensor(job_definition, name=None):
    """
    The decorated function will be called as the ``should_execute_fn`` of the underlying
    :py:class:`~dagster.ScheduleDefinition` and should take a :py:class:`~dagster.ScheduleContext`
    as its only argument, returning a boolean if the execution should fire

    Args:
        execution (str): The execution to execute when the sensor fires.
        name (str): The name of this sensor
    """
    check.inst_param(job_definition, "job_definition", JobDefinition)
    check.opt_str_param(name, "name")

    def inner(fn):
        check.callable_param(fn, "fn")
        name = name or fn.__name__

        return SensorDefinition(name=name, job_definition=job_definition, should_execute=fn)

    return inner
