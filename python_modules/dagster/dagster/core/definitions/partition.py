import inspect
from abc import ABC, abstractmethod
from collections import namedtuple
from datetime import datetime
from typing import Callable, List, NamedTuple, Optional, cast

import pendulum
from dagster import check
from dagster.core.definitions.run_request import RunRequest, SkipReason
from dagster.core.definitions.schedule import ScheduleDefinition, ScheduleExecutionContext
from dagster.core.errors import (
    DagsterInvalidDefinitionError,
    DagsterInvariantViolationError,
    ScheduleExecutionError,
    user_code_error_boundary,
)
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus, PipelineRunsFilter
from dagster.core.storage.tags import check_tags
from dagster.seven import PendulumDateTime, to_timezone
from dagster.utils import frozenlist, merge_dicts
from dagster.utils.schedules import schedule_execution_time_iterator

from .mode import DEFAULT_MODE_NAME
from .utils import check_valid_name

DEFAULT_DATE_FORMAT = "%Y-%m-%d"


class Partition(namedtuple("_Partition", ("value name"))):
    """
    Partition is the representation of a logical slice across an axis of a pipeline's work

    Args:
        value (Any): The object for this partition
        name (str): Name for this partition
    """

    def __new__(cls, value=None, name=None):
        return super(Partition, cls).__new__(
            cls, name=check.opt_str_param(name, "name", str(value)), value=value
        )


def last_empty_partition(context, partition_set_def):
    check.inst_param(context, "context", ScheduleExecutionContext)
    partition_set_def = check.inst_param(
        partition_set_def, "partition_set_def", PartitionSetDefinition
    )

    partitions = partition_set_def.get_partitions(context.scheduled_execution_time)
    if not partitions:
        return None
    selected = None
    for partition in reversed(partitions):
        filters = PipelineRunsFilter.for_partition(partition_set_def, partition)
        matching = context.instance.get_runs(filters)
        if not any(run.status == PipelineRunStatus.SUCCESS for run in matching):
            selected = partition
            break
    return selected


def schedule_partition_range(
    start,
    end,
    cron_schedule,
    fmt,
    timezone,
    execution_time_to_partition_fn,
    inclusive=False,
):
    check.inst_param(start, "start", datetime)
    check.opt_inst_param(end, "end", datetime)
    check.str_param(cron_schedule, "cron_schedule")
    check.str_param(fmt, "fmt")
    check.opt_str_param(timezone, "timezone")
    check.callable_param(execution_time_to_partition_fn, "execution_time_to_partition_fn")
    check.opt_bool_param(inclusive, "inclusive")

    if end and start > end:
        raise DagsterInvariantViolationError(
            'Selected date range start "{start}" is after date range end "{end}'.format(
                start=start.strftime(fmt),
                end=end.strftime(fmt),
            )
        )

    def _get_schedule_range_partitions(current_time=None):
        check.opt_inst_param(current_time, "current_time", datetime)
        tz = timezone if timezone else "UTC"
        _start = (
            to_timezone(start, tz)
            if isinstance(start, PendulumDateTime)
            else pendulum.instance(start, tz=tz)
        )

        if end:
            _end = end
        elif current_time:
            _end = current_time
        else:
            _end = pendulum.now(tz)

        # coerce to the definition timezone
        if isinstance(_end, PendulumDateTime):
            _end = to_timezone(_end, tz)
        else:
            _end = pendulum.instance(_end, tz=tz)

        end_timestamp = _end.timestamp()

        partitions = []
        for next_time in schedule_execution_time_iterator(_start.timestamp(), cron_schedule, tz):

            partition_time = execution_time_to_partition_fn(next_time)

            if partition_time.timestamp() > end_timestamp:
                break

            if partition_time.timestamp() < _start.timestamp():
                continue

            partitions.append(Partition(value=partition_time, name=partition_time.strftime(fmt)))

        return partitions if inclusive else partitions[:-1]

    return _get_schedule_range_partitions


class PartitionParams(ABC):
    @abstractmethod
    def get_partitions(self, current_time: Optional[datetime]) -> List[Partition]:
        """Retrieve the partitions at a given evaluation time.

        Args:
            current_time (Optional[datetime]): The evaluation time when generating the partitions.

        Returns:
            List[Partition]: A list of :py:class:`~dagster.Partition` repesenting logical slices
                of a pipeline's work.
        """


class StaticPartitionParams(
    PartitionParams, NamedTuple("_StaticPartitionParams", [("partitions", List[Partition])])
):
    def __new__(cls, partitions: List[Partition]):
        return super(StaticPartitionParams, cls).__new__(
            cls, check.list_param(partitions, "partitions", of_type=Partition)
        )

    def get_partitions(self, current_time: Optional[datetime]) -> List[Partition]:
        return self.partitions


class TimeBasedPartitionParams(
    PartitionParams,
    NamedTuple(
        "_TimeBasedPartitionParams",
        [
            ("start", datetime),
            ("execution_minute", Optional[int]),
            ("execution_hour", Optional[int]),
            ("execution_day_of_week", Optional[int]),
            ("execution_day_of_month", Optional[int]),
            ("execution_month", Optional[int]),
            ("end", Optional[datetime]),
            ("fmt", Optional[str]),
            ("timezone", Optional[str]),
            ("partition_minutes_offset", Optional[int]),
            ("partition_hours_offset", Optional[int]),
            ("partition_days_offset", Optional[int]),
            ("partition_weeks_offset", Optional[int]),
            ("partition_months_offset", Optional[int]),
        ],
    ),
):
    def __new__(
        cls,
        start: datetime,
        execution_minute: Optional[int] = None,
        execution_hour: Optional[int] = None,
        execution_day_of_week: Optional[int] = None,
        execution_day_of_month: Optional[int] = None,
        execution_month: Optional[int] = None,
        end: Optional[datetime] = None,
        fmt: Optional[str] = None,
        timezone: Optional[str] = None,
        partition_minutes_offset: Optional[int] = None,
        partition_hours_offset: Optional[int] = None,
        partition_days_offset: Optional[int] = None,
        partition_weeks_offset: Optional[int] = None,
        partition_months_offset: Optional[int] = None,
    ):
        check.invariant(
            end is None or start <= end,
            f'Selected date range start "{start}" '
            f'is after date range end "{end}"'.format(
                start=start.strftime(fmt) if fmt is not None else start,
                end=cast(datetime, end).strftime(fmt)
                if fmt is not None and end is not None
                else end,
            ),
        )
        check.invariant(
            execution_minute is None or 0 <= execution_minute <= 59,
            f'Execution minute "{execution_minute}" must be between 0 and 59.',
        )
        check.invariant(
            execution_hour is None or 0 <= execution_hour <= 23,
            f'Execution hour "{execution_hour}" must be between 0 and 23.',
        )
        check.invariant(
            execution_day_of_week is None or 0 <= execution_day_of_week <= 6,
            f'Execution day of week "{execution_day_of_week}" must be between 0 and 6.',
        )
        check.invariant(
            execution_day_of_month is None or 1 <= execution_day_of_month <= 31,
            f'Execution day of month "{execution_day_of_month}" must be between 1 and 31.',
        )
        check.invariant(
            execution_month is None or 1 <= execution_month <= 12,
            f'Execution month "{execution_day_of_month}" must be between 1 and 12.',
        )
        check.invariant(
            partition_minutes_offset is None or partition_minutes_offset > 0,
            f'Partition minutes offset "{partition_minutes_offset}" must be greater than 0.',
        )
        check.invariant(
            partition_hours_offset is None or partition_hours_offset > 0,
            f'Partition hours offset "{partition_hours_offset}" must be greater than 0.',
        )
        check.invariant(
            partition_days_offset is None or partition_days_offset > 0,
            f'Partition days offset "{partition_days_offset}" must be greater than 0.',
        )
        check.invariant(
            partition_weeks_offset is None or partition_weeks_offset > 0,
            f'Partition weeks offset "{partition_weeks_offset}" must be greater than 0.',
        )
        check.invariant(
            partition_months_offset is None or partition_months_offset > 0,
            f'Partition months offset "{partition_months_offset}" must be greater than 0.',
        )

        return super(TimeBasedPartitionParams, cls).__new__(
            cls,
            check.inst_param(start, "start", datetime),
            check.opt_int_param(execution_minute, "execution_minute"),
            check.opt_int_param(execution_hour, "execution_hour"),
            check.opt_int_param(execution_day_of_week, "execution_day_of_week"),
            check.opt_int_param(execution_day_of_month, "execution_day_of_month"),
            check.opt_int_param(execution_month, "execution_month"),
            check.opt_inst_param(end, "end", datetime),
            check.opt_str_param(fmt, "fmt", default=DEFAULT_DATE_FORMAT),
            check.opt_str_param(timezone, "timezone", default="UTC"),
            check.opt_int_param(partition_minutes_offset, "partition_minutes_offset", 0),
            check.opt_int_param(partition_hours_offset, "partition_hours_offset", 0),
            check.opt_int_param(partition_days_offset, "partition_days_offset", 0),
            check.opt_int_param(partition_weeks_offset, "partition_weeks_offset", 0),
            check.opt_int_param(partition_months_offset, "partition_months_offset", 0),
        )

    def get_partitions(self, current_time: Optional[datetime]) -> List[Partition]:
        check.opt_inst_param(current_time, "current_time", datetime)

        partition_fn = schedule_partition_range(
            start=self.start,
            end=self.end,
            cron_schedule=self._get_cron_schedule(),
            fmt=self.fmt,
            timezone=self.timezone,
            execution_time_to_partition_fn=self._get_execution_time_to_partition_fn(),
            inclusive=self._inclusive(),
        )

        return partition_fn(current_time=current_time)

    def _inclusive(self):
        return not any(
            (
                self.partition_minutes_offset,
                self.partition_hours_offset,
                self.partition_days_offset,
                self.partition_weeks_offset,
                self.partition_months_offset,
            )
        )

    def _get_cron_schedule(self) -> str:
        minute = self.execution_minute if self.execution_minute is not None else "*"
        hour = self.execution_hour if self.execution_hour is not None else "*"
        day_of_week = self.execution_day_of_week if self.execution_day_of_week is not None else "*"
        day_of_month = (
            self.execution_day_of_month if self.execution_day_of_month is not None else "*"
        )
        month = self.execution_month if self.execution_month is not None else "*"

        cron_schedule = f"{minute} {hour} {day_of_month} {month} {day_of_week}"

        return cron_schedule

    def _get_execution_time_to_partition_fn(self) -> Callable[[datetime], datetime]:
        return lambda d: pendulum.instance(d).subtract(
            months=self.partition_months_offset,
            weeks=self.partition_weeks_offset,
            days=self.partition_days_offset,
            hours=self.partition_hours_offset,
            minutes=self.partition_minutes_offset,
        )


class DynamicPartitionParams(
    PartitionParams,
    NamedTuple(
        "_DynamicPartitionParams",
        [("partition_fn", Callable[[Optional[datetime]], List[Partition]])],
    ),
):
    def __new__(cls, partition_fn: Callable[[Optional[datetime]], List[Partition]]):
        return super(DynamicPartitionParams, cls).__new__(
            cls, check.callable_param(partition_fn, "partition_fn")
        )

    def get_partitions(self, current_time: Optional[datetime]) -> List[Partition]:
        return self.partition_fn(current_time)


class PartitionSetDefinition(
    namedtuple(
        "_PartitionSetDefinition",
        (
            "name pipeline_name partition_fn solid_selection mode "
            "user_defined_run_config_fn_for_partition user_defined_tags_fn_for_partition "
            "partition_params"
        ),
    )
):
    """
    Defines a partition set, representing the set of slices making up an axis of a pipeline

    Args:
        name (str): Name for this partition set
        pipeline_name (str): The name of the pipeline definition
        partition_fn (Optional[Callable[void, List[Partition]]]): User-provided function to define
            the set of valid partition objects.
        solid_selection (Optional[List[str]]): A list of solid subselection (including single
            solid names) to execute with this partition. e.g. ``['*some_solid+', 'other_solid']``
        mode (Optional[str]): The mode to apply when executing this partition. (default: 'default')
        run_config_fn_for_partition (Callable[[Partition], [Dict]]): A
            function that takes a :py:class:`~dagster.Partition` and returns the run
            configuration that parameterizes the execution for this partition, as a dict
        tags_fn_for_partition (Callable[[Partition], Optional[dict[str, str]]]): A function that
            takes a :py:class:`~dagster.Partition` and returns a list of key value pairs that will
            be added to the generated run for this partition.
        partition_params (Optional[PartitionParams]): A set of parameters used to construct the set
            of valid partition objects.
    """

    def __new__(
        cls,
        name,
        pipeline_name,
        partition_fn=None,
        solid_selection=None,
        mode=None,
        run_config_fn_for_partition=lambda _partition: {},
        tags_fn_for_partition=lambda _partition: {},
        partition_params=None,
    ):
        check.invariant(
            partition_fn is not None or partition_params is not None,
            "One of `partition_fn` or `partition_params` must be supplied.",
        )

        if partition_fn is not None:
            partition_fn_param_count = len(inspect.signature(partition_fn).parameters)

            def _wrap_partition(x):
                if isinstance(x, Partition):
                    return x
                if isinstance(x, str):
                    return Partition(x)
                raise DagsterInvalidDefinitionError(
                    "Expected <Partition> | <str>, received {type}".format(type=type(x))
                )

            def _wrap_partition_fn(current_time=None):
                if not current_time:
                    current_time = pendulum.now("UTC")

                check.callable_param(partition_fn, "partition_fn")

                if partition_fn_param_count == 1:
                    obj_list = partition_fn(current_time)
                else:
                    obj_list = partition_fn()

                return [_wrap_partition(obj) for obj in obj_list]

        return super(PartitionSetDefinition, cls).__new__(
            cls,
            name=check_valid_name(name),
            pipeline_name=check.str_param(pipeline_name, "pipeline_name"),
            partition_fn=partition_fn,
            solid_selection=check.opt_nullable_list_param(
                solid_selection, "solid_selection", of_type=str
            ),
            mode=check.opt_str_param(mode, "mode", DEFAULT_MODE_NAME),
            user_defined_run_config_fn_for_partition=check.callable_param(
                run_config_fn_for_partition, "run_config_fn_for_partition"
            ),
            user_defined_tags_fn_for_partition=check.callable_param(
                tags_fn_for_partition, "tags_fn_for_partition"
            ),
            partition_params=check.opt_inst_param(
                partition_params,
                "partition_params",
                PartitionParams,
                default=DynamicPartitionParams(partition_fn=_wrap_partition_fn)
                if partition_fn is not None
                else None,
            ),
        )

    def run_config_for_partition(self, partition):
        return self.user_defined_run_config_fn_for_partition(partition)

    def tags_for_partition(self, partition):
        user_tags = self.user_defined_tags_fn_for_partition(partition)
        check_tags(user_tags, "user_tags")

        tags = merge_dicts(user_tags, PipelineRun.tags_for_partition_set(self, partition))

        return tags

    def get_partitions(self, current_time=None):
        """Return the set of known partitions.

        Arguments:
            current_time (Optional[datetime]): The evaluation time for the partition function, which
                is passed through to the ``partition_fn`` (if it accepts a parameter).  Defaults to
                the current time in UTC.
        """
        return self.partition_params.get_partitions(current_time)

    def get_partition(self, name):
        for partition in self.get_partitions():
            if partition.name == name:
                return partition

        check.failed("Partition name {} not found!".format(name))

    def get_partition_names(self, current_time=None):
        return [part.name for part in self.get_partitions(current_time)]

    def create_schedule_definition(
        self,
        schedule_name,
        cron_schedule,
        partition_selector,
        should_execute=None,
        environment_vars=None,
        execution_timezone=None,
        description=None,
    ):
        """Create a ScheduleDefinition from a PartitionSetDefinition.

        Arguments:
            schedule_name (str): The name of the schedule.
            cron_schedule (str): A valid cron string for the schedule
            partition_selector (Callable[ScheduleExecutionContext, PartitionSetDefinition], Union[Partition, List[Partition]]):
                Function that determines the partition to use at a given execution time. Can return
                either a single Partition or a list of Partitions. For time-based partition sets,
                will likely be either `identity_partition_selector` or a selector returned by
                `create_offset_partition_selector`.
            should_execute (Optional[function]): Function that runs at schedule execution time that
                determines whether a schedule should execute. Defaults to a function that always returns
                ``True``.
            environment_vars (Optional[dict]): The environment variables to set for the schedule.
            execution_timezone (Optional[str]): Timezone in which the schedule should run. Only works
                with DagsterDaemonScheduler, and must be set when using that scheduler.
            description (Optional[str]): A human-readable description of the schedule.

        Returns:
            PartitionScheduleDefinition: The generated PartitionScheduleDefinition for the partition
                selector
        """

        check.str_param(schedule_name, "schedule_name")
        check.str_param(cron_schedule, "cron_schedule")
        check.opt_callable_param(should_execute, "should_execute")
        check.opt_dict_param(environment_vars, "environment_vars", key_type=str, value_type=str)
        check.callable_param(partition_selector, "partition_selector")
        check.opt_str_param(execution_timezone, "execution_timezone")
        check.opt_str_param(description, "description")

        def _execution_fn(context):
            check.inst_param(context, "context", ScheduleExecutionContext)
            with user_code_error_boundary(
                ScheduleExecutionError,
                lambda: f"Error occurred during the execution of partition_selector for schedule {schedule_name}",
            ):
                selector_result = partition_selector(context, self)

            if isinstance(selector_result, SkipReason):
                yield selector_result
                return

            selected_partitions = (
                selector_result
                if isinstance(selector_result, (frozenlist, list))
                else [selector_result]
            )

            check.is_list(selected_partitions, of_type=Partition)

            if not selected_partitions:
                yield SkipReason("Partition selector returned an empty list of partitions.")
                return

            missing_partition_names = [
                partition.name
                for partition in selected_partitions
                if partition.name not in self.get_partition_names(context.scheduled_execution_time)
            ]

            if missing_partition_names:
                yield SkipReason(
                    "Partition selector returned partition"
                    + ("s" if len(missing_partition_names) > 1 else "")
                    + f" not in the partition set: {', '.join(missing_partition_names)}."
                )
                return

            with user_code_error_boundary(
                ScheduleExecutionError,
                lambda: f"Error occurred during the execution of should_execute for schedule {schedule_name}",
            ):
                if should_execute and not should_execute(context):
                    yield SkipReason(
                        "should_execute function for {schedule_name} returned false.".format(
                            schedule_name=schedule_name
                        )
                    )
                    return

            for selected_partition in selected_partitions:
                with user_code_error_boundary(
                    ScheduleExecutionError,
                    lambda: f"Error occurred during the execution of run_config_fn for schedule {schedule_name}",
                ):
                    run_config = self.run_config_for_partition(selected_partition)

                with user_code_error_boundary(
                    ScheduleExecutionError,
                    lambda: f"Error occurred during the execution of tags_fn for schedule {schedule_name}",
                ):
                    tags = self.tags_for_partition(selected_partition)
                yield RunRequest(
                    run_key=selected_partition.name if len(selected_partitions) > 0 else None,
                    run_config=run_config,
                    tags=tags,
                )

        return PartitionScheduleDefinition(
            name=schedule_name,
            cron_schedule=cron_schedule,
            pipeline_name=self.pipeline_name,
            tags_fn=None,
            solid_selection=self.solid_selection,
            mode=self.mode,
            should_execute=None,
            environment_vars=environment_vars,
            partition_set=self,
            execution_timezone=execution_timezone,
            execution_fn=_execution_fn,
            description=description,
        )


class PartitionScheduleDefinition(ScheduleDefinition):
    __slots__ = ["_partition_set"]

    def __init__(
        self,
        name,
        cron_schedule,
        pipeline_name,
        tags_fn,
        solid_selection,
        mode,
        should_execute,
        environment_vars,
        partition_set,
        run_config_fn=None,
        execution_timezone=None,
        execution_fn=None,
        description=None,
    ):
        super(PartitionScheduleDefinition, self).__init__(
            name=check_valid_name(name),
            cron_schedule=cron_schedule,
            pipeline_name=pipeline_name,
            run_config_fn=run_config_fn,
            tags_fn=tags_fn,
            solid_selection=solid_selection,
            mode=mode,
            should_execute=should_execute,
            environment_vars=environment_vars,
            execution_timezone=execution_timezone,
            execution_fn=execution_fn,
            description=description,
        )
        self._partition_set = check.inst_param(
            partition_set, "partition_set", PartitionSetDefinition
        )

    def get_partition_set(self):
        return self._partition_set
