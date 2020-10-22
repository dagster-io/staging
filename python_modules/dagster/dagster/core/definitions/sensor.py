from datetime import datetime

from dagster import check
from dagster.core.definitions.job import JobContext, JobDefinition
from dagster.core.instance import DagsterInstance
from dagster.utils.backcompat import experimental_class_warning


class SensorExecutionContext(JobContext):
    """Sensor execution context.

    An instance of this class is made available as the first argument to the `should_execute`
    function on SensorDefinition.

    Attributes:
        instance (DagsterInstance): The instance configured to run the schedule
        last_execution_time (datetime): The last time that the sensor initiated execution.
    """

    __slots__ = ["_last_execution_time"]

    def __init__(self, instance, last_execution_time):
        super(SensorExecutionContext, self).__init__(
            check.inst_param(instance, "instance", DagsterInstance),
        )
        self._last_execution_time = check.opt_inst_param(
            last_execution_time, "last_execution_time", datetime
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
