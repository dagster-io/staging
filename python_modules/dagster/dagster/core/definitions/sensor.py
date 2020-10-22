from collections import namedtuple
from datetime import datetime

from dagster import check
from dagster.core.definitions.job import JobDefinition
from dagster.core.instance import DagsterInstance
from dagster.utils.backcompat import experimental_class_warning


class SensorExecutionContext(namedtuple("SensorExecutionContext", "instance last_execution_time")):
    """Sensor execution context.

    An instance of this class is made available as the first argument to the `should_execute`
    function on SensorDefinition.

    Attributes:
        instance (DagsterInstance): The instance configured to run the schedule
        last_execution_time (datetime): The last time that the sensor initiated execution.
    """

    def __new__(cls, instance, last_execution_time):

        return super(SensorExecutionContext, cls).__new__(
            cls,
            check.inst_param(instance, "instance", DagsterInstance),
            check.opt_inst_param(last_execution_time, "scheduled_execution_time", datetime),
        )


class SensorDefinition(object):
    def __init__(self, name, job_definition, should_execute):
        experimental_class_warning("SensorDefinition")
        self._name = check.str_param(name, "name")
        self._job = check.inst_param(job_definition, "job_definition", JobDefinition)
        self._should_execute = check.callable_param(should_execute, "should_execute")

    @property
    def name(self):
        return self._name

    @property
    def job(self):
        return self._job

    def should_execute(self, context):
        check.inst_param(context, "context", SensorExecutionContext)
        return self._should_execute(context)
