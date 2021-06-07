import os
from datetime import datetime

import pytest
from dagster import Partition
from dagster.core.definitions import PipelineDefinition
from dagster.core.definitions.partition import PartitionScheduleDefinition
from dagster.core.execution.api import create_execution_plan
from hacker_news.pipelines.download_pipeline import download_pipeline, dynamic_download_pipeline
from hacker_news.schedules.hourly_hn_download_schedule import (
    hourly_hn_download_prod_schedule,
    hourly_hn_download_staging_schedule,
    hourly_hn_dynamic_download_prod_schedule,
)


def assert_partitioned_schedule_builds(
    schedule_def: PartitionScheduleDefinition,
    pipeline_def: PipelineDefinition,
    partition: datetime,
):
    run_config = schedule_def.get_partition_set().run_config_for_partition(Partition(partition))
    create_execution_plan(pipeline_def, run_config=run_config, mode=schedule_def.mode)


@pytest.mark.parametrize(
    "schedule",
    [
        hourly_hn_download_staging_schedule,
        hourly_hn_download_prod_schedule,
    ],
)
def test_daily_download_schedule(schedule):
    os.environ["SLACK_DAGSTER_ETL_BOT_TOKEN"] = "something"
    assert_partitioned_schedule_builds(
        schedule, download_pipeline, datetime.strptime("2020-10-01", "%Y-%m-%d")
    )


def test_daily_dynamic_download_schedule():
    os.environ["SLACK_DAGSTER_ETL_BOT_TOKEN"] = "something"
    assert_partitioned_schedule_builds(
        hourly_hn_dynamic_download_prod_schedule,
        dynamic_download_pipeline,
        datetime.strptime("2020-10-01", "%Y-%m-%d"),
    )
