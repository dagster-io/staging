from datetime import datetime, time
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Union

import pendulum
from dagster import check

from ...utils.schedules import schedule_execution_time_iterator
from .partition import (
    DEFAULT_DATE_FORMAT,
    Partition,
    PartitionedConfig,
    PartitionsDefinition,
    ScheduleType,
    get_cron_schedule,
)


class TimeWindow(NamedTuple):
    """An interval that is closed at the start and open at the end"""

    start: datetime
    end: datetime


class TimeWindowPartitionsDefinition(
    PartitionsDefinition[TimeWindow],  # pylint: disable=unsubscriptable-object
    NamedTuple(
        "_TimeWindowPartitions",
        [
            ("schedule_type", ScheduleType),
            ("start", datetime),
            ("timezone", str),
            ("fmt", str),
            ("execution_time", time),
            ("execution_day", Optional[int]),
        ],
    ),
):
    def get_partitions(
        self, current_time: Optional[datetime] = None
    ) -> List[Partition[TimeWindow]]:
        current_timestamp = (
            pendulum.instance(current_time, tz=self.timezone)
            if current_time
            else pendulum.now(self.timezone)
        ).timestamp()

        start_timestamp = pendulum.instance(self.start, tz=self.timezone).timestamp()
        iterator = schedule_execution_time_iterator(
            start_timestamp=start_timestamp,
            cron_schedule=get_cron_schedule(
                schedule_type=self.schedule_type,
                time_of_day=self.execution_time,
                day_of_week=self.execution_day,
            ),
            execution_timezone=self.timezone,
        )

        partitions: List[Partition[TimeWindow]] = []
        prev_time = next(iterator)
        while prev_time.timestamp() <= current_timestamp:
            next_time = next(iterator)

            if (
                prev_time.timestamp() >= start_timestamp
                and next_time.timestamp() <= current_timestamp
            ):
                partitions.append(
                    Partition(
                        value=TimeWindow(prev_time, next_time), name=prev_time.strftime(self.fmt)
                    )
                )

            prev_time = next_time

        return partitions


def daily_partitioned_config(
    start_date: Union[datetime, str],
    timezone: Optional[str] = None,
    fmt: Optional[str] = None,
    execution_time: time = time(0, 0),
) -> Callable[[Callable[[datetime, datetime], Dict[str, Any]]], PartitionedConfig]:
    _fmt = fmt or DEFAULT_DATE_FORMAT
    _timezone = timezone or "UTC"

    if isinstance(start_date, str):
        _start_date = datetime.strptime(start_date, _fmt)
    else:
        _start_date = start_date

    def inner(fn: Callable[[datetime, datetime], Dict[str, Any]]) -> PartitionedConfig:
        check.callable_param(fn, "fn")

        return PartitionedConfig(
            run_config_for_partition_fn=lambda partition: fn(
                partition.value[0], partition.value[1]
            ),
            partitions_def=TimeWindowPartitionsDefinition(
                schedule_type=ScheduleType.DAILY,
                start=_start_date,
                timezone=_timezone,
                fmt=_fmt,
                execution_time=execution_time,
                execution_day=None,
            ),
        )

    return inner


def hourly_partitioned_config(
    start_date: Union[datetime, str],
    timezone: Optional[str] = None,
    fmt: Optional[str] = None,
    execution_time: time = time(0, 0),
) -> Callable[[Callable[[datetime, datetime], Dict[str, Any]]], PartitionedConfig]:
    _fmt = fmt or DEFAULT_DATE_FORMAT
    _timezone = timezone or "UTC"

    if isinstance(start_date, str):
        _start_date = datetime.strptime(start_date, _fmt)
    else:
        _start_date = start_date

    def inner(fn: Callable[[datetime, datetime], Dict[str, Any]]) -> PartitionedConfig:
        check.callable_param(fn, "fn")

        return PartitionedConfig(
            run_config_for_partition_fn=lambda partition: fn(
                partition.value[0], partition.value[1]
            ),
            partitions_def=TimeWindowPartitionsDefinition(
                schedule_type=ScheduleType.HOURLY,
                start=_start_date,
                timezone=_timezone,
                fmt=_fmt,
                execution_time=execution_time,
                execution_day=None,
            ),
        )

    return inner


def monthly_partitioned_config(
    start_date: Union[datetime, str],
    timezone: Optional[str] = None,
    fmt: Optional[str] = None,
    execution_time: time = time(0, 0),
    execution_day_of_month: int = 1,
) -> Callable[[Callable[[datetime, datetime], Dict[str, Any]]], PartitionedConfig]:
    _fmt = fmt or DEFAULT_DATE_FORMAT
    _timezone = timezone or "UTC"

    if isinstance(start_date, str):
        _start_date = datetime.strptime(start_date, _fmt)
    else:
        _start_date = start_date

    def inner(fn: Callable[[datetime, datetime], Dict[str, Any]]) -> PartitionedConfig:
        check.callable_param(fn, "fn")

        return PartitionedConfig(
            run_config_for_partition_fn=lambda partition: fn(
                partition.value[0], partition.value[1]
            ),
            partitions_def=TimeWindowPartitionsDefinition(
                schedule_type=ScheduleType.MONTHLY,
                start=_start_date,
                timezone=_timezone,
                fmt=_fmt,
                execution_time=execution_time,
                execution_day=execution_day_of_month,
            ),
        )

    return inner


def weekly_partitioned_config(
    start_date: Union[datetime, str],
    timezone: Optional[str] = None,
    fmt: Optional[str] = None,
    execution_time: time = time(0, 0),
    execution_day_of_week: int = 0,
) -> Callable[[Callable[[datetime, datetime], Dict[str, Any]]], PartitionedConfig]:
    _fmt = fmt or DEFAULT_DATE_FORMAT
    _timezone = timezone or "UTC"

    if isinstance(start_date, str):
        _start_date = datetime.strptime(start_date, _fmt)
    else:
        _start_date = start_date

    def inner(fn: Callable[[datetime, datetime], Dict[str, Any]]) -> PartitionedConfig:
        check.callable_param(fn, "fn")

        return PartitionedConfig(
            run_config_for_partition_fn=lambda partition: fn(
                partition.value[0], partition.value[1]
            ),
            partitions_def=TimeWindowPartitionsDefinition(
                schedule_type=ScheduleType.WEEKLY,
                start=_start_date,
                timezone=_timezone,
                fmt=_fmt,
                execution_time=execution_time,
                execution_day=execution_day_of_week,
            ),
        )

    return inner
