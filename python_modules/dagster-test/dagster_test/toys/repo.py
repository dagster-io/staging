from dagster import repository
from dagster_test.toys.asset_lineage import asset_lineage_partition_set, asset_lineage_pipeline
from dagster_test.toys.branches import sleep_branch_job, sleep_failed_branch_job
from dagster_test.toys.composition import composition_graph
from dagster_test.toys.dynamic import dynamic_pipeline
from dagster_test.toys.error_monster import error_monster_job
from dagster_test.toys.hammer import hammer_pipeline
from dagster_test.toys.log_asset import log_asset_job
from dagster_test.toys.log_file import log_file_job
from dagster_test.toys.log_s3 import log_s3_job
from dagster_test.toys.log_spew import log_spew_job
from dagster_test.toys.longitudinal import longitudinal_job
from dagster_test.toys.many_events import many_events_job
from dagster_test.toys.retries import retry_job
from dagster_test.toys.sleepy import sleepy_job
from dagster_test.toys.unreliable import unreliable_job

from .schedules import get_toys_schedules
from .sensors import get_toys_sensors


@repository
def toys_repository():
    return (
        [
            composition_graph,
            error_monster_job,
            hammer_pipeline,
            log_asset_job,
            log_file_job,
            log_s3_job,
            log_spew_job,
            longitudinal_job,
            many_events_job,
            sleepy_job,
            retry_job,
            sleep_branch_job,
            sleep_failed_branch_job,
            unreliable_job,
            dynamic_pipeline,
            asset_lineage_pipeline,
            asset_lineage_partition_set,
        ]
        + get_toys_schedules()
        + get_toys_sensors()
    )
