import datetime
from collections import defaultdict

from dagster import (
    PartitionSetDefinition,
    ScheduleExecutionContext,
    daily_schedule,
    hourly_schedule,
    monthly_schedule,
    weekly_schedule,
)
from dagster.core.definitions.schedule import ScheduleDefinition
from dagster.core.storage.fs_io_manager import fs_io_manager
from dagster.utils.partitions import date_partition_range
from dagster_test.toys.many_events import many_events_job


def _toys_tz_info():
    # Provides execution_timezone information, which is only used to determine execution time when
    # the scheduler configured is the DagsterCommandLineScheduler
    return "US/Pacific"


@hourly_schedule(
    pipeline_name="?",
    start_date=datetime.datetime(2021, 1, 1),
    execution_timezone=_toys_tz_info(),
    job=many_events_job,
)
def hourly_materialization_schedule():
    return {}


@daily_schedule(
    pipeline_name="?",
    start_date=datetime.datetime(2021, 1, 1),
    execution_timezone=_toys_tz_info(),
    job=many_events_job,
)
def daily_materialization_schedule():
    return {}


@weekly_schedule(
    pipeline_name="?",
    start_date=datetime.datetime(2021, 1, 1),
    execution_timezone=_toys_tz_info(),
    job=many_events_job,
)
def weekly_materialization_schedule():
    return {}


@monthly_schedule(
    pipeline_name="?",
    start_date=datetime.datetime(2021, 1, 1),
    execution_timezone=_toys_tz_info(),
    job=many_events_job,
)
def monthly_materialization_schedule():
    return {}


def longitudinal_schedule():
    from .longitudinal import longitudinal_graph

    def longitudinal_config(partition):
        return {
            "solids": {
                solid.name: {"config": {"partition": partition.name}}
                for solid in longitudinal_graph.solids
            }
        }

    # partition_set = PartitionSetDefinition(
    #     name="ingest_and_train",
    #     pipeline_name="longitudinal_pipeline",
    #     partition_fn=date_partition_range(start=datetime.datetime(2020, 1, 1)),
    #     run_config_fn_for_partition=longitudinal_config,
    # )

    # def _should_execute(context):
    #     return backfill_should_execute(context, partition_set, retry_failed=True)

    # def _partition_selector(context, partition_set):
    #     return backfilling_partition_selector(context, partition_set, retry_failed=True)

    # return partition_set.create_schedule_definition(
    #     schedule_name=schedule_name,
    #     cron_schedule="*/5 * * * *",  # tick every 5 minutes
    #     partition_selector=_partition_selector,
    #     should_execute=_should_execute,
    #     execution_timezone=_toys_tz_info(),
    # )

    job = longitudinal_graph.to_job(
        name="longitudinal_demo",
        resource_defs={"io_manager": fs_io_manager},
        config_mapping=longitudinal_config,
        partitions=date_partition_range(start=datetime.datetime(2020, 1, 1)),
    )

    return ScheduleDefinition(
        name="longitudinal_demo_schedule",
        job=job,
        cron_schedule="*/5 * * * *",  # tick every 5 minutes,
        execution_timezone=_toys_tz_info(),
    )


def many_events_schedule():
    return ScheduleDefinition(
        name="many_events_every_min",
        cron_schedule="* * * * *",
        job=many_events_job,
        run_config_fn=lambda _: {"intermediate_storage": {"filesystem": {}}},
        execution_timezone=_toys_tz_info(),
    )


def get_toys_schedules():

    return [
        longitudinal_schedule(),
        many_events_schedule(),
        hourly_materialization_schedule,
        daily_materialization_schedule,
        weekly_materialization_schedule,
        monthly_materialization_schedule,
    ]
