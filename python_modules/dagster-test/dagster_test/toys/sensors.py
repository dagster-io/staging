import os

from dagster.core.definitions.decorators.sensor import sensor
from dagster.core.definitions.job import JobConfig
from dagster_aws.s3.sensor import s3_sensor

from .schedules import longitudinal_partition_set


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

    bucket = os.environ.get("DAGSTER_AWS_SENSOR_BUCKET")

    @s3_sensor(pipeline_name="log_s3_pipeline", bucket=bucket, default_interval=300)
    def aws_s3_log(modified_s3_objects):
        return [
            JobConfig(
                run_config={
                    "solids": {"log_path": {"config": {"bucket": bucket, "path": obj["Key"]}}}
                }
            )
            for obj in modified_s3_objects
        ]

    return [
        event_sensor,
        aws_s3_log,
        longitudinal_partition_set.create_sequential_backfill_sensor(
            "longitudinal_backfill_sensor"
        ),
    ]
