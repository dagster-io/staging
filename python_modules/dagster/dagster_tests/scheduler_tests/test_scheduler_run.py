import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta

from freezegun import freeze_time

from dagster import daily_schedule, hourly_schedule, pipeline, repository, solid
from dagster.core.host_representation import PythonEnvRepositoryLocation, RepositoryLocationHandle
from dagster.core.scheduler import ScheduleTickStatus
from dagster.core.storage.pipeline_run import PipelineRunStatus
from dagster.core.storage.tags import PARTITION_NAME_TAG, SCHEDULED_EXECUTION_TIME_TAG
from dagster.core.test_utils import instance_for_test
from dagster.core.types.loadable_target_origin import LoadableTargetOrigin
from dagster.scheduler.scheduler import launch_scheduled_runs
from dagster.seven import get_timestamp_from_utc_datetime, get_utc_timezone

_COUPLE_DAYS_AGO = datetime(year=2019, month=2, day=25)


def _throw(_context):
    raise Exception("bananas")


def _never(_context):
    return False


@solid(config_schema={"work_amt": str})
def the_solid(context):
    return "0.8.0 was {} of work".format(context.solid_config["work_amt"])


@pipeline
def the_pipeline():
    the_solid()


@daily_schedule(pipeline_name="the_pipeline", start_date=_COUPLE_DAYS_AGO)
def simple_schedule(_context):
    return {"solids": {"the_solid": {"config": {"work_amt": "a lot"}}}}


@daily_schedule(pipeline_name="the_pipeline", start_date=_COUPLE_DAYS_AGO)
def bad_env_fn_schedule():  # forgot context arg
    return {}


@hourly_schedule(pipeline_name="the_pipeline", start_date=_COUPLE_DAYS_AGO)
def simple_hourly_schedule(_context):
    return {"solids": {"the_solid": {"config": {"work_amt": "even more"}}}}


@daily_schedule(
    pipeline_name="the_pipeline", start_date=_COUPLE_DAYS_AGO, should_execute=_throw,
)
def bad_should_execute_schedule(_context):
    return {}


@daily_schedule(
    pipeline_name="the_pipeline", start_date=_COUPLE_DAYS_AGO, should_execute=_never,
)
def skip_schedule(_context):
    return {}


@daily_schedule(
    pipeline_name="the_pipeline", start_date=_COUPLE_DAYS_AGO,
)
def wrong_config_schedule(_context):
    return {}


@repository
def the_repo():
    return [
        the_pipeline,
        simple_schedule,
        simple_hourly_schedule,
        bad_env_fn_schedule,
        bad_should_execute_schedule,
        skip_schedule,
        wrong_config_schedule,
    ]


@contextmanager
def instance_with_schedules(external_repo):
    with instance_for_test() as instance:
        instance.reconcile_scheduler_state(external_repo)
        yield instance


def get_external_repo():
    loadable_target_origin = LoadableTargetOrigin(
        executable_path=sys.executable, python_file=__file__, attribute="the_repo",
    )
    return PythonEnvRepositoryLocation(
        RepositoryLocationHandle.create_python_env_location(
            loadable_target_origin=loadable_target_origin, location_name="test_location",
        )
    ).get_repository("the_repo")


def _validate_tick(tick, external_schedule, expected_datetime, expected_status, expected_run_id):
    tick_data = tick.schedule_tick_data
    assert tick_data.schedule_origin_id == external_schedule.get_origin_id()
    assert tick_data.schedule_name == external_schedule.name
    assert tick_data.cron_schedule == external_schedule.cron_schedule
    assert tick_data.timestamp == get_timestamp_from_utc_datetime(expected_datetime)
    assert tick_data.status == expected_status
    assert tick_data.run_id == expected_run_id


def _validate_run_started(run, expected_datetime, expected_partition):
    assert run.tags[SCHEDULED_EXECUTION_TIME_TAG] == expected_datetime.isoformat()
    assert run.tags[PARTITION_NAME_TAG] == expected_partition
    assert run.status == PipelineRunStatus.STARTED or run.status == PipelineRunStatus.SUCCESS


def _wait_for_all_runs_to_start(instance, timeout=10):
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            raise Exception("Timed out waiting for runs to start")
        time.sleep(0.5)

        not_started_runs = [
            run for run in instance.get_runs() if run.status == PipelineRunStatus.NOT_STARTED
        ]

        if len(not_started_runs) == 0:
            break


def test_launch_scheduled_execution():
    initial_datetime = datetime(
        year=2019, month=2, day=27, hour=23, minute=59, second=59, tzinfo=get_utc_timezone(),
    )
    with freeze_time(initial_datetime) as frozen_datetime:
        external_repo = get_external_repo()
        with instance_with_schedules(external_repo) as instance:
            external_schedule = external_repo.get_external_schedule("simple_schedule")

            schedule_origin = external_schedule.get_origin()

            instance.start_schedule_and_update_storage_state(external_schedule)

            assert instance.get_runs_count() == 0
            ticks = instance.get_schedule_ticks(schedule_origin.get_id())
            assert len(ticks) == 0

            # launch_scheduled_runs does nothing before the first tick
            launch_scheduled_runs(instance)
            assert instance.get_runs_count() == 0
            ticks = instance.get_schedule_ticks(schedule_origin.get_id())
            assert len(ticks) == 0

            # Move forward in time so we're past a tick
            frozen_datetime.tick(delta=timedelta(seconds=2))
            launch_scheduled_runs(instance)

            assert instance.get_runs_count() == 1
            ticks = instance.get_schedule_ticks(schedule_origin.get_id())
            assert len(ticks) == 1

            expected_datetime = datetime(year=2019, month=2, day=28, tzinfo=get_utc_timezone())

            _validate_tick(
                ticks[0],
                external_schedule,
                expected_datetime,
                ScheduleTickStatus.SUCCESS,
                instance.get_runs()[0].run_id,
            )

            _wait_for_all_runs_to_start(instance)
            _validate_run_started(instance.get_runs()[0], expected_datetime, "2019-02-27")

            # Verify idempotence
            launch_scheduled_runs(instance)
            assert instance.get_runs_count() == 1
            ticks = instance.get_schedule_ticks(schedule_origin.get_id())
            assert len(ticks) == 1
            assert ticks[0].status == ScheduleTickStatus.SUCCESS

            # Verify advancing in time but not going past a tick doesn't add any new runs
            frozen_datetime.tick(delta=timedelta(seconds=2))
            launch_scheduled_runs(instance)
            assert instance.get_runs_count() == 1
            ticks = instance.get_schedule_ticks(schedule_origin.get_id())
            assert len(ticks) == 1
            assert ticks[0].status == ScheduleTickStatus.SUCCESS

            # Traveling two more days in the future before running results in two new ticks
            frozen_datetime.tick(delta=timedelta(days=2))
            launch_scheduled_runs(instance)
            assert instance.get_runs_count() == 3
            ticks = instance.get_schedule_ticks(schedule_origin.get_id())
            assert len(ticks) == 3
            assert len([tick for tick in ticks if tick.status == ScheduleTickStatus.SUCCESS]) == 3

            runs_by_partition = {run.tags[PARTITION_NAME_TAG]: run for run in instance.get_runs()}

            assert "2019-02-28" in runs_by_partition
            assert "2019-03-01" in runs_by_partition

            # Check idempotence again
            launch_scheduled_runs(instance)
            assert instance.get_runs_count() == 3
            ticks = instance.get_schedule_ticks(schedule_origin.get_id())
            assert len(ticks) == 3


def test_run_scheduled_on_time_boundary():
    external_repo = get_external_repo()
    with instance_with_schedules(external_repo) as instance:
        external_schedule = external_repo.get_external_schedule("simple_schedule")

        schedule_origin = external_schedule.get_origin()
        initial_datetime = datetime(
            year=2019, month=2, day=27, hour=0, minute=0, second=0, tzinfo=get_utc_timezone(),
        )
        with freeze_time(initial_datetime):
            # Start schedule exactly at midnight
            instance.start_schedule_and_update_storage_state(external_schedule)

            launch_scheduled_runs(instance)

            assert instance.get_runs_count() == 1
            ticks = instance.get_schedule_ticks(schedule_origin.get_id())
            assert len(ticks) == 1
            assert ticks[0].status == ScheduleTickStatus.SUCCESS


def test_multiple_schedules_on_different_time_ranges():
    external_repo = get_external_repo()
    with instance_with_schedules(external_repo) as instance:
        external_schedule = external_repo.get_external_schedule("simple_schedule")
        external_hourly_schedule = external_repo.get_external_schedule("simple_hourly_schedule")
        initial_datetime = datetime(
            year=2019, month=2, day=27, hour=23, minute=59, second=59, tzinfo=get_utc_timezone(),
        )
        with freeze_time(initial_datetime) as frozen_datetime:
            instance.start_schedule_and_update_storage_state(external_schedule)
            instance.start_schedule_and_update_storage_state(external_hourly_schedule)
            frozen_datetime.tick(delta=timedelta(seconds=2))

            launch_scheduled_runs(instance)

            assert instance.get_runs_count() == 2
            ticks = instance.get_schedule_ticks(external_schedule.get_origin_id())
            assert len(ticks) == 1
            assert ticks[0].status == ScheduleTickStatus.SUCCESS

            hourly_ticks = instance.get_schedule_ticks(external_hourly_schedule.get_origin_id())
            assert len(hourly_ticks) == 1
            assert hourly_ticks[0].status == ScheduleTickStatus.SUCCESS

            frozen_datetime.tick(delta=timedelta(hours=1))

            launch_scheduled_runs(instance)

            assert instance.get_runs_count() == 3

            ticks = instance.get_schedule_ticks(external_schedule.get_origin_id())
            assert len(ticks) == 1
            assert ticks[0].status == ScheduleTickStatus.SUCCESS

            hourly_ticks = instance.get_schedule_ticks(external_hourly_schedule.get_origin_id())
            assert len(hourly_ticks) == 2
            assert (
                len([tick for tick in hourly_ticks if tick.status == ScheduleTickStatus.SUCCESS])
                == 2
            )


# More things to test here before this diff lands:
# - more information about the ticks and the runs (the tag, the status, that they're running, etc.)
# - case when the scheduler was interrupted in the middle of a tick
# - case where a run was created but scheduler was interrupted or crashed before it could launch
# - retry after interrupt in the middle of a tick
# - verify an error in one schedule doesn't affect execution of another schedule, or the next tick in the schedule
# - schedule execution data error
# - skipped schedule
# - error while fetching execution plan for run
# - interval command on execute_scheduler_command, actual execute_scheduler_command call
