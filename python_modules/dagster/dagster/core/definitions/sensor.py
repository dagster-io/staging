from dagster import check
from dagster.core.instance import DagsterInstance
from dagster.utils import ensure_gen
from dagster.utils.backcompat import experimental_class_warning

from .job import JobContext, JobDefinition, JobType, RunParams, RunSkippedData


class SensorExecutionContext(JobContext):
    """Sensor execution context.

    An instance of this class is made available as the first argument to the evaluation function
    on SensorDefinition.

    Attributes:
        instance (DagsterInstance): The instance configured to run the schedule
        last_evaluation_time (float): The last time that the sensor was evaluated (UTC).
    """

    __slots__ = ["_last_evaluation_time"]

    def __init__(self, instance, last_evaluation_time):
        super(SensorExecutionContext, self).__init__(
            check.inst_param(instance, "instance", DagsterInstance),
        )
        self._last_evaluation_time = check.opt_float_param(
            last_evaluation_time, "last_evaluation_time"
        )

    @property
    def last_evaluation_time(self):
        return self._last_evaluation_time


class SensorDefinition(JobDefinition):
    """Define a sensor that initiates a set of job runs

    Args:
        name (str): The name of the sensor to create.
        pipeline_name (str): The name of the pipeline to execute when the sensor fires.
        evaluation_fn (Callable[[SensorExecutionContext]]): The core evaluation function for the
            sensor, which is run at an interval to determine whether a run should be launched or
            not. Takes a :py:class:`~dagster.SensorExecutionContext`.

            This function must return a generator, which must yield either a single RunSkippedData
            or one or more RunParams objects.
        solid_selection (Optional[List[str]]): A list of solid subselection (including single
            solid names) to execute when the sensor runs. e.g. ``['*some_solid+', 'other_solid']``
        mode (Optional[str]): The mode to apply when executing this sensor. (default: 'default')
    """

    __slots__ = [
        "_evaluation_fn",
    ]

    def __init__(
        self, name, pipeline_name, evaluation_fn, solid_selection=None, mode=None,
    ):
        experimental_class_warning("SensorDefinition")
        super(SensorDefinition, self).__init__(
            name,
            job_type=JobType.SENSOR,
            pipeline_name=pipeline_name,
            mode=mode,
            solid_selection=solid_selection,
        )
        self._evaluation_fn = check.callable_param(evaluation_fn, "evaluation_fn")

    def get_execution_data(self, context):
        check.inst_param(context, "context", SensorExecutionContext)
        result = list(ensure_gen(self._evaluation_fn(context)))

        if not result or result == [None]:
            return []

        if len(result) == 1:
            return check.is_list(result, of_type=(RunParams, RunSkippedData))

        return check.is_list(result, of_type=RunParams)
