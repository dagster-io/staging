from typing import Generator

from dagster import RunRequest, SensorExecutionContext, SkipReason, sensor


@sensor(pipeline_name="my_pipeline_A")
def my_sensor_A(_context: SensorExecutionContext) -> Generator[RunRequest, SkipReason, None]:
    should_run = False
    if should_run:
        yield RunRequest(run_key=None, run_config={})
