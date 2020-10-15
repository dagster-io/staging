from dagster import instigation_rule, PipelineRun
from date_util import date_range, today
from typing import Dict
from iterator import pairwise


my_pipeline = None


def config_for_partition(partition: str) -> Dict:
    pass


@instigation_rule()
def partition_based_schedule(context):
    executable_pipeline_name = "my_pipeline_prod"
    for partition in date_range("2018-10-01", today()):
        if not context.has_completed_or_queued_runs(
            name=executable_pipeline_name, tags=[partition]
        ):
            yield PipelineRun(
                my_pipeline, tags={partition: None}, run_config=config_for_partition(partition),
            )


@instigation_rule()
def backfill(context):
    backfill_id = "abc123"
    executable_pipeline_name = "my_pipeline_prod"
    for partition in date_range("2018-10-01", "2019-10-05"):
        if not context.has_completed_or_queued_runs(
            name=executable_pipeline_name, tags=[backfill_id, partition]
        ):
            yield PipelineRun(
                my_pipeline,
                tags={backfill_id: None, partition: None},
                run_config=config_for_partition(partition),
            )


@instigation_rule()
def sequential_backfill(context):
    backfill_id = "abc123"
    executable_pipeline_name = "my_pipeline_prod"

    for partition, prev_partition in pairwise(date_range("2018-10-01", "2019-10-05")):
        has_run = context.has_completed_or_queued_runs(
            name=executable_pipeline_name, tags=[backfill_id, partition]
        )
        has_required_run = context.has_completed_runs(
            name=executable_pipeline_name, tags=[backfill_id, prev_partition]
        )

        if not has_run and has_required_run:
            yield PipelineRun(
                my_pipeline,
                tags={backfill_id: None, partition: None},
                run_config=config_for_partition(partition),
            )


@instigation_rule()
def asset_based():
    pass


@instigation_rule
def cross_dag():
    pass


@instigation_rule()
def one_run_per_file(context):
    pass
