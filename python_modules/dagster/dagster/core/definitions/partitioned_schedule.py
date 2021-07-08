from datetime import time
from typing import Optional, Union, cast

from dagster import check

from .partition import (
    Partition,
    PartitionSetDefinition,
    PartitionedConfig,
    ScheduleTimeBasedPartitionsDefinition,
    ScheduleType,
)
from .pipeline import PipelineDefinition
from .run_request import SkipReason
from .schedule import ScheduleDefinition, ScheduleEvaluationContext
from .time_window_partitions import TimeWindow, TimeWindowPartitionsDefinition


def schedule_from_partitions(
    job: PipelineDefinition,
    description: Optional[str] = None,
    name: Optional[str] = None,
    minute_of_hour: Optional[time] = None,
    time_of_day: Optional[time] = None,
    day_of_week: Optional[int] = None,
    day_of_month: Optional[int] = None,
) -> ScheduleDefinition:
    """
    Creates a schedule from a time window-partitioned job.

    The schedule executes at the cadence specified by the partitioning of the given job.
    """
    check.invariant(len(job.mode_definitions) == 1, "job must only have one mode")

    check.invariant(
        job.mode_definitions[0].partitioned_config is not None, "job must be a partitioned job"
    )

    check.invariant(
        not (day_of_week and day_of_month),
        "Cannot provide both day_of_month and day_of_week parameter to schedule_from_partitions.",
    )

    partitioned_config = cast(PartitionedConfig, job.mode_definitions[0].partitioned_config)

    partition_set = PartitionSetDefinition(
        name=f"{job.name}_partitions",
        pipeline_name=job.name,
        run_config_fn_for_partition=partitioned_config.run_config_for_partition_fn,
        mode=job.mode_definitions[0].name,
        partitions_def=partitioned_config.partitions_def,
    )

    check.inst(partitioned_config.partitions_def, TimeWindowPartitionsDefinition)
    partitions_def = cast(TimeWindowPartitionsDefinition, partitioned_config.partitions_def)

    if partitions_def.schedule_type == ScheduleType.HOURLY:
        execution_time = check.opt_inst_param(
            minute_of_hour, "minute_of_hour", time, default=time(0, 0)
        )
    else:
        execution_time = check.opt_inst_param(time_of_day, "time_of_day", time, default=time(0, 0))

    if partitions_def.schedule_type == ScheduleType.MONTHLY:
        execution_day = check.opt_int_param(day_of_month, "day_of_month", default=1)
    elif partitions_def.schedule_type == ScheduleType.WEEKLY:
        execution_day = check.opt_int_param(day_of_week, "day_of_week", default=0)
    else:
        execution_day = 0

    schedule_partitions = ScheduleTimeBasedPartitionsDefinition(
        schedule_type=partitions_def.schedule_type,
        start=partitions_def.start,
        execution_time=execution_time,
        execution_day=execution_day,
        offset=1,
    )

    schedule_def = partition_set.create_schedule_definition(
        schedule_name=check.opt_str_param(name, "name", f"{job.name}_schedule"),
        cron_schedule=schedule_partitions.get_cron_schedule(),
        partition_selector=latest_window_partition_selector,
        execution_timezone=partitions_def.timezone,
        description=description,
        job=job,
    )

    return schedule_def


def latest_window_partition_selector(
    context: ScheduleEvaluationContext, partition_set_def: PartitionSetDefinition[TimeWindow]
) -> Union[SkipReason, Partition[TimeWindow]]:
    """Creates a selector for partitions that are time windows. Selects latest time window that ends
    before the schedule tick time.
    """
    partitions = partition_set_def.get_partitions(context.scheduled_execution_time)
    if len(partitions) == 0:
        return SkipReason()
    else:
        return partitions[-1]
