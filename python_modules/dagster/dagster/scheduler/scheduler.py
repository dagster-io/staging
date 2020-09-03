import sys
from contextlib import contextmanager
from datetime import datetime

import click
from croniter import croniter_range

from dagster import check
from dagster.core.errors import DagsterLaunchFailedError
from dagster.core.events import EngineEventData
from dagster.core.host_representation import (
    ExternalPipeline,
    ExternalScheduleExecutionErrorData,
    PipelineSelector,
    RepositoryLocation,
    RepositoryLocationHandle,
)
from dagster.core.instance import DagsterInstance
from dagster.core.scheduler import (
    ScheduleState,
    ScheduleStatus,
    ScheduleTickData,
    ScheduleTickStatus,
)
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus, PipelineRunsFilter
from dagster.core.storage.tags import check_tags
from dagster.grpc.types import ScheduleExecutionDataMode
from dagster.seven import get_utc_timezone
from dagster.utils import merge_dicts
from dagster.utils.error import serializable_error_info_from_exc_info


class _ScheduleTickHolder:
    def __init__(self, tick, instance):
        self._tick = tick
        self._instance = instance
        self._set = False

    @property
    def status(self):
        return self._tick.status

    def update_with_status(self, status, **kwargs):
        self._tick = self._tick.with_status(status=status, **kwargs)

    def write(self):
        self._instance.update_schedule_tick(self._tick)


@contextmanager
def _schedule_tick_state(instance, tick_data):
    # TODO This is not efficient, should avoid loading all ticks for a schedule at oncce.
    ticks_at_time = [
        tick
        for tick in instance.get_schedule_ticks(tick_data.schedule_origin_id)
        if tick.timestamp == tick_data.timestamp
    ]
    if ticks_at_time:
        check.invariant(len(ticks_at_time) == 1)
        tick = ticks_at_time[0]
    else:
        tick = instance.create_schedule_tick(tick_data)

    holder = _ScheduleTickHolder(tick=tick, instance=instance)
    try:
        yield holder
    except Exception as e:  # pylint: disable=broad-except
        # Don't write a tick failure (and keep the schedule from being re-run by the scheduler)
        # if the process is interrupted
        if not isinstance(e, KeyboardInterrupt):
            error_data = serializable_error_info_from_exc_info(sys.exc_info())
            holder.update_with_status(ScheduleTickStatus.FAILURE, error=error_data)
    finally:
        holder.write()


@click.command(name="scheduler", help="Launch all scheduled runs that are past their start time")
def scheduler_command():
    # TODO: Need a clear answer for the exact deployment of this command. Do we need a parent
    # process that periodically launches this command in a subprocess? What kind of monitoring
    # do we need to provide vs. assuming that the user provides?
    instance = DagsterInstance.get()

    schedules = [
        s for s in instance.all_stored_schedule_state() if s.status == ScheduleStatus.RUNNING
    ]

    for schedule_state in schedules:
        launch_scheduled_runs(instance, schedule_state)


def launch_scheduled_runs(instance, schedule_state):
    check.inst_param(instance, "instance", DagsterInstance)
    check.inst_param(schedule_state, "schedule_state", ScheduleState)

    # TODO support non-UTC timezones here, add exhaustive timezone and DST testing.
    #
    # TODO Save the last_execution_timestamp when we succesfully complete a run as an optimization
    # to keep from needing to scan through the whole time range
    start_time_utc = datetime.fromtimestamp(schedule_state.start_timestamp, tz=get_utc_timezone())

    for schedule_time_utc in croniter_range(
        start_time_utc, datetime.now(get_utc_timezone()), schedule_state.cron_schedule
    ):
        _schedule_run_at_time(
            instance, schedule_state, schedule_time_utc,
        )


def _schedule_run_at_time(
    instance, schedule_state, schedule_time_utc,
):
    schedule_origin = schedule_state.origin
    schedule_name = schedule_state.name

    with _schedule_tick_state(
        instance,
        ScheduleTickData(
            schedule_origin_id=schedule_state.schedule_origin_id,
            schedule_name=schedule_state.name,
            timestamp=schedule_time_utc.timestamp(),
            cron_schedule=schedule_state.cron_schedule,
            status=ScheduleTickStatus.STARTED,
        ),
    ) as tick:
        if tick.status != ScheduleTickStatus.STARTED:
            # Tick for this execution time previously ran to completion, don't try again
            return

        # TODO We could cache the external artifacts per-schedule, particularly if we end
        # up implementing a catchup feature and are likely to be queuing multiple runs per
        # scheduler loop. If we do this we need to make sure that load failure errors are correctly
        # logged in the tick DB.
        repo_location = RepositoryLocation.from_handle(
            RepositoryLocationHandle.create_from_repository_origin(
                schedule_origin.repository_origin, instance,
            )
        )

        repo_dict = repo_location.get_repositories()
        check.invariant(
            len(repo_dict) == 1,
            "Reconstructed repository location should have exactly one repository",
        )
        external_repo = next(iter(repo_dict.values()))

        external_schedule = external_repo.get_external_schedule(schedule_name)

        pipeline_selector = PipelineSelector(
            location_name=repo_location.name,
            repository_name=external_repo.name,
            pipeline_name=external_schedule.pipeline_name,
            solid_selection=external_schedule.solid_selection,
        )

        subset_pipeline_result = repo_location.get_subset_external_pipeline_result(
            pipeline_selector
        )
        external_pipeline = ExternalPipeline(
            subset_pipeline_result.external_pipeline_data, external_repo.handle,
        )

        # Rule out the case where the scheduler crashed between creating a run for this time
        # and launching it
        runs_filter = PipelineRunsFilter(
            tags=merge_dicts(
                PipelineRun.tags_for_schedule(schedule_state),
                {".dagster/scheduled_execution_time": schedule_time_utc.isoformat()},
            )
        )
        existing_runs = instance.run_storage.get_runs(runs_filter)

        run_to_launch = None

        if len(existing_runs):
            check.invariant(len(existing_runs) == 1)

            run = existing_runs[0]

            if run.status != PipelineRunStatus.NOT_STARTED:
                # A run already exists and was launched for this time period,
                # but the scheduler must have crashed before the tick could be put
                # into a SUCCESS state
                return
            run_to_launch = run
        else:
            run_to_launch = _create_scheduler_run(
                instance,
                schedule_time_utc,
                repo_location,
                external_repo,
                external_schedule,
                external_pipeline,
                tick,
            )

        try:
            instance.launch_run(run_to_launch.run_id, external_pipeline)
        except DagsterLaunchFailedError:
            error = serializable_error_info_from_exc_info(sys.exc_info())
            instance.report_engine_event(
                error.message, run_to_launch, EngineEventData.engine_error(error),
            )
            instance.report_run_failed(run_to_launch.run_id)
            raise

        tick.update_with_status(ScheduleTickStatus.SUCCESS, run_id=run_to_launch.run_id)


def _create_scheduler_run(
    instance,
    schedule_time_utc,
    repo_location,
    external_repo,
    external_schedule,
    external_pipeline,
    tick,
):
    # TODO we should pass in the schedule_time_utc here, let partition-less schedules know their
    # intended execution time rather than needing to guess
    schedule_execution_data = repo_location.get_external_schedule_execution_data(
        instance=instance,
        repository_handle=external_repo.handle,
        schedule_name=external_schedule.name,
        schedule_execution_data_mode=ScheduleExecutionDataMode.LAUNCH_SCHEDULED_EXECUTION,
    )

    if isinstance(schedule_execution_data, ExternalScheduleExecutionErrorData):
        error = schedule_execution_data.error
        tick.update_with_status(ScheduleTickStatus.FAILURE, error=error)
        return
    elif not schedule_execution_data.should_execute:
        # Update tick to skipped state and return
        tick.update_with_status(ScheduleTickStatus.SKIPPED)
        return

    # This does not keep the behavior from the cron scheduler where
    # a failure loading the execution plan or tags still creates a run but then
    # marks the tick as failed, as it's particularly tricky to keep idempotence
    # working with that operation

    run_config = schedule_execution_data.run_config
    schedule_tags = schedule_execution_data.tags
    external_execution_plan = repo_location.get_external_execution_plan(
        external_pipeline, run_config, external_schedule.mode, step_keys_to_execute=None,
    )
    execution_plan_snapshot = external_execution_plan.execution_plan_snapshot

    pipeline_tags = external_pipeline.tags or {}
    check_tags(pipeline_tags, "pipeline_tags")
    tags = merge_dicts(pipeline_tags, schedule_tags)

    tags[".dagster/scheduled_execution_time"] = schedule_time_utc.isoformat()

    return instance.create_run(
        pipeline_name=external_schedule.pipeline_name,
        run_id=None,
        run_config=run_config,
        mode=external_schedule.mode,
        solids_to_execute=external_pipeline.solids_to_execute,
        step_keys_to_execute=None,
        solid_selection=external_pipeline.solid_selection,
        status=None,
        root_run_id=None,
        parent_run_id=None,
        tags=tags,
        pipeline_snapshot=external_pipeline.pipeline_snapshot,
        execution_plan_snapshot=execution_plan_snapshot,
        parent_pipeline_snapshot=external_pipeline.parent_pipeline_snapshot,
    )
