from datetime import datetime

from dagster import ScheduleExecutionContext, hourly_schedule


@hourly_schedule(
    pipeline_name="my_pipeline", start_date=datetime(2021, 1, 1), execution_timezone="US/Central",
)
def my_hourly_schedule(_context: ScheduleExecutionContext) -> dict:
    return {}
