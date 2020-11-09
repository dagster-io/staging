import os

from dagster import check
from dagster.core.definitions.decorators.sensor import sensor
from dagster.core.definitions.job import JobConfig
from dagster.core.definitions.partition import PartitionSetDefinition
from dagster.core.definitions.sensor import SensorDefinition
from dagster.core.storage.pipeline_run import PipelineRunStatus, PipelineRunsFilter
from dagster.core.storage.tags import BACKFILL_ID_TAG, PARTITION_NAME_TAG
from dagster_aws.s3.sensor import s3_sensor

from .schedules import longitudinal_partition_set


def sequential_backfill_sensor(name, partition_set_def):
    check.str_param(name, "name")
    check.inst_param(partition_set_def, "partition_set_def", PartitionSetDefinition)

    def _execution_fn(context):
        if not context.last_evaluation_time:
            return []

        runs = context.instance.get_runs(
            PipelineRunsFilter(
                pipeline_name=partition_set_def.pipeline_name,
                status=PipelineRunStatus.SUCCESS,
                tag_keys=[PARTITION_NAME_TAG, BACKFILL_ID_TAG],
            ),
            limit=1,
        )

        if not runs:
            return []

        run = runs[0]
        partition_name = run.tags.get(PARTITION_NAME_TAG)
        backfill_name = run.tags.get(BACKFILL_ID_TAG)

        last_matched = False
        next_partition = None

        partitions = partition_set_def.get_partitions()
        for partition in partitions:
            if last_matched:
                next_partition = partition
            elif partition.name == partition_name:
                last_matched = True

        if not next_partition:
            return []

        existing_runs = context.instance.get_runs(
            PipelineRunsFilter(
                pipeline_name=partition_set_def.pipeline_name,
                status=PipelineRunStatus.SUCCESS,
                tags={PARTITION_NAME_TAG: next_partition.name, BACKFILL_ID_TAG: backfill_name},
            ),
            limit=1,
        )

        if existing_runs:
            return []

        return [
            JobConfig(
                run_config=partition_set_def.run_config_for_partition(next_partition),
                tags=partition_set_def.tags_for_partition(next_partition),
            )
        ]

    return SensorDefinition(
        name=name,
        pipeline_name=partition_set_def.name,
        mode=partition_set_def.mode,
        job_config_fn=_execution_fn,
        solid_selection=partition_set_def.solid_selection,
    )


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

    longitudinal_backfill_sensor = sequential_backfill_sensor(
        "longitudinal_backfill_sensor", longitudinal_partition_set
    )

    return [event_sensor, aws_s3_log, longitudinal_backfill_sensor]
