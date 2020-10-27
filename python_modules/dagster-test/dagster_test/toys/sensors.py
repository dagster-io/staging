import os
from dagster.core.definitions.decorators.sensor import sensor


def get_toys_sensors():
    filepath = os.environ.get("DAGSTER_SENSOR_DEMO")

    @sensor(pipeline_name="many_events")
    def event_sensor(context):
        try:
            mtime = os.path.getmtime(filepath)
        except OSError:
            return False

        return not context.last_checked_time or mtime > context.last_checked_time

    return [event_sensor]
