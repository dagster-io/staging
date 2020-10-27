import os
from dagster.core.definitions.decorators.sensor import sensor
from dagster.core.definitions.job import JobConfig


def get_toys_sensors():
    filepath = os.environ.get("DAGSTER_SENSOR_DEMO")

    @sensor(pipeline_name="many_events")
    def event_sensor(context):
        try:
            mtime = os.path.getmtime(filepath)
        except OSError:
            return []

        if not context.last_checked_time or mtime > context.last_checked_time:
            return [JobConfig(run_config={}, tags={}, execution_key=str(mtime))]

        return []

    return [event_sensor]
