import inspect
from abc import ABC, abstractmethod
from collections import namedtuple
from datetime import datetime
from typing import Callable, List, NamedTuple, Optional, cast

import pendulum
from dagster import check
from dagster.core.definitions.job import RunRequest, SkipReason
from dagster.core.definitions.schedule import ScheduleDefinition, ScheduleExecutionContext
from dagster.core.errors import (
    DagsterInvalidDefinitionError,
    ScheduleExecutionError,
    user_code_error_boundary,
)
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus, PipelineRunsFilter
from dagster.core.storage.tags import check_tags
from dagster.seven import PendulumDateTime, to_timezone
from dagster.utils import merge_dicts

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


class PartitionParams(ABC):
    @abstractmethod
    def get_partitions(self, current_time: datetime) -> List[Partition]:
        ...


class StaticPartitionParams(
    PartitionParams, NamedTuple("_StaticPartitionParams", [("partitions", List[Partition])])
):
    def __new__(cls, partitions: List[Partition]):
        return super(StaticPartitionParams, cls).__new__(
            cls, check.list_param(partitions, "partitions", of_type=Partition)
        )

    def get_partitions(self, _current_time: datetime) -> List[Partition]:
        return self.partitions


class TimeBasedPartitionParams(
    PartitionParams,
    NamedTuple(
        "_TimeBasedPartitionParams",
        [
            ("start", datetime),
            ("end", Optional[datetime]),
            ("delta_range", str),
            ("delta_amount", Optional[int]),
            ("fmt", Optional[str]),
            ("inclusive", Optional[bool]),
            ("timezone", Optional[str]),
        ],
    ),
):
    def __new__(
        cls,
        start: datetime,
        end: Optional[datetime],
        delta_range: str,
        delta_amount: Optional[int],
        fmt: Optional[str],
        inclusive: Optional[bool],
        timezone: Optional[str],
    ):
        check.invariant(
            not (end and start > end),
            f'Selected date range start "{start}" '
            f'is after date range end "{end}"'.format(
                start=start.strftime(fmt) if fmt is not None else start,
                end=cast(datetime, end).strftime(fmt) if fmt is not None else end,
            ),
        )
        return super(TimeBasedPartitionParams, cls).__new__(
            cls,
            check.inst_param(start, "start", datetime),
            check.opt_inst_param(end, "end", datetime),
            check.str_param(delta_range, "delta_range"),
            check.opt_int_param(delta_amount, "delta_amount", 1),
            check.opt_str_param(fmt, "fmt", default=DEFAULT_DATE_FORMAT),
            check.opt_bool_param(inclusive, "inclusive", default=False),
            check.opt_str_param(timezone, "timezone", default="UTC"),
        )

    def get_partitions(self, current_time: datetime) -> List[Partition]:
        check.opt_inst_param(current_time, "current_time", datetime)
        _start = (
            to_timezone(self.start, self.timezone)
            if isinstance(self.start, PendulumDateTime)
            else pendulum.instance(self.start, tz=self.timezone)
        )

        if self.end:
            _end = self.end
        elif current_time:
            _end = current_time
        else:
            _end = pendulum.now(self.timezone)

        # coerce to the definition timezone
        if isinstance(_end, PendulumDateTime):
            _end = to_timezone(_end, self.timezone)
        else:
            _end = pendulum.instance(_end, tz=self.timezone)

        period = pendulum.period(_start, _end)
        date_names = [
            Partition(value=current, name=current.strftime(self.fmt))
            for current in period.range(self.delta_range, self.delta_amount)
        ]

        # We don't include the last element here by default since we only want
        # fully completed intervals, and the _end time is in the middle of the interval
        # represented by the last element of date_names
        return date_names if self.inclusive else date_names[:-1]


class DynamicPartitionParams(
    PartitionParams,
    NamedTuple(
        "_DynamicPartitionParams", [("partition_fn", Callable[[datetime], List[Partition]])]
    ),
):
    def __new__(cls, partition_fn: Callable[[datetime], List[Partition]]):
        return super(DynamicPartitionParams, cls).__new__(
            cls, check.callable_param(partition_fn, "partition_fn")
        )

    def get_partitions(self, current_time: datetime) -> List[Partition]:
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
        partition_fn (Callable[void, List[Partition]]): User-provided function to define the set of
            valid partition objects.
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
        partition_fn,
        solid_selection=None,
        mode=None,
        run_config_fn_for_partition=lambda _partition: {},
        tags_fn_for_partition=lambda _partition: {},
        partition_params=None,
    ):
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
            partition_fn=_wrap_partition_fn,
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
                partition_params, "partition_params", PartitionParams
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
        partition_params = (
            DynamicPartitionParams(self.partition_fn)
            if self.partition_params is None
            else self.partition_params
        )
        return partition_params.get_partitions(current_time)

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
            partition_selector (Callable[ScheduleExecutionContext, PartitionSetDefinition],
            Partition): Function that determines the partition to use at a given execution time.
            For time-based partition sets, will likely be either `identity_partition_selector` or a
            selector returned by `create_offset_partition_selector`.
            should_execute (Optional[function]): Function that runs at schedule execution time that
            determines whether a schedule should execute. Defaults to a function that always returns
            ``True``.
            environment_vars (Optional[dict]): The environment variables to set for the schedule.
            execution_timezone (Optional[str]): Timezone in which the schedule should run. Only works
                with DagsterDaemonScheduler, and must be set when using that scheduler.
            description (Optional[str]): A human-readable description of the schedule.

        Returns:
            ScheduleDefinition: The generated ScheduleDefinition for the partition selector
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
                selected_partition = partition_selector(context, self)

            if isinstance(selected_partition, SkipReason):
                yield selected_partition
                return

            if selected_partition.name not in self.get_partition_names(
                context.scheduled_execution_time
            ):
                yield SkipReason(
                    f"Partition selector returned a partition {selected_partition.name} not in the partition set."
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
                run_key=None,
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
