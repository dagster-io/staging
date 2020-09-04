from __future__ import print_function

import os
import sys
from contextlib import contextmanager

import click
import six
from croniter import croniter_range

from dagster import DagsterInvariantViolationError, check
from dagster.cli.workspace.cli_target import (
    get_external_repository_from_kwargs,
    get_external_repository_from_repo_location,
    location_handle_from_load_target,
    repository_target_argument,
)
from dagster.core.errors import DagsterLaunchFailedError
from dagster.core.events import EngineEventData
from dagster.core.host_representation import (
    ExternalPipeline,
    ExternalRepository,
    RepositoryLocation,
)
from dagster.core.host_representation.external_data import ExternalScheduleExecutionErrorData
from dagster.core.host_representation.selector import PipelineSelector
from dagster.core.instance import DagsterInstance
from dagster.core.scheduler import (
    ScheduleStatus,
    ScheduleTickData,
    ScheduleTickStatus,
    get_schedule_change_set,
)
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus, PipelineRunsFilter
from dagster.core.storage.tags import check_tags
from dagster.grpc.types import ScheduleExecutionDataMode
from dagster.seven import get_current_datetime_in_utc
from dagster.utils.error import serializable_error_info_from_exc_info
from dagster.utils.merger import merge_dicts


def create_schedule_cli_group():
    group = click.Group(name="schedule")
    group.add_command(schedule_list_command)
    group.add_command(schedule_up_command)
    group.add_command(schedule_preview_command)
    group.add_command(schedule_start_command)
    group.add_command(schedule_logs_command)
    group.add_command(schedule_stop_command)
    group.add_command(schedule_restart_command)
    group.add_command(schedule_wipe_command)
    group.add_command(schedule_debug_command)
    return group


def print_changes(external_repository, instance, print_fn=print, preview=False):
    debug_info = instance.scheduler_debug_info()
    errors = debug_info.errors
    external_schedules = external_repository.get_external_schedules()
    changeset = get_schedule_change_set(
        instance.all_stored_schedule_state(external_repository.get_origin_id()), external_schedules
    )

    if len(errors) == 0 and len(changeset) == 0:
        if preview:
            print_fn(click.style("No planned changes to schedules.", fg="magenta", bold=True))
            print_fn("{num} schedules will remain unchanged".format(num=len(external_schedules)))
        else:
            print_fn(click.style("No changes to schedules.", fg="magenta", bold=True))
            print_fn("{num} schedules unchanged".format(num=len(external_schedules)))
        return

    if len(errors):
        print_fn(
            click.style(
                "Planned Error Fixes:" if preview else "Errors Resolved:", fg="magenta", bold=True
            )
        )
        print_fn("\n".join(debug_info.errors))

    if len(changeset):
        print_fn(
            click.style(
                "Planned Schedule Changes:" if preview else "Changes:", fg="magenta", bold=True
            )
        )

    for change in changeset:
        change_type, schedule_name, schedule_origin_id, changes = change

        if change_type == "add":
            print_fn(
                click.style(
                    "  + {name} (add) [{id}]".format(name=schedule_name, id=schedule_origin_id),
                    fg="green",
                )
            )

        if change_type == "change":
            print_fn(
                click.style(
                    "  ~ {name} (update) [{id}]".format(name=schedule_name, id=schedule_origin_id),
                    fg="yellow",
                )
            )
            for change_name, diff in changes:
                if len(diff) == 2:
                    old, new = diff
                    print_fn(
                        click.style("\t %s: " % change_name, fg="yellow")
                        + click.style(old, fg="red")
                        + " => "
                        + click.style(new, fg="green")
                    )
                else:
                    print_fn(
                        click.style("\t %s: " % change_name, fg="yellow")
                        + click.style(diff, fg="green")
                    )

        if change_type == "remove":
            print_fn(
                click.style(
                    "  - {name} (delete) [{id}]".format(name=schedule_name, id=schedule_origin_id),
                    fg="red",
                )
            )


def check_repo_and_scheduler(repository, instance):
    check.inst_param(repository, "repository", ExternalRepository)
    check.inst_param(instance, "instance", DagsterInstance)

    repository_name = repository.name

    if not repository.get_external_schedules():
        raise click.UsageError(
            "There are no schedules defined for repository {name}.".format(name=repository_name)
        )

    if not instance.scheduler:
        raise click.UsageError(
            "A scheduler must be configured to run schedule commands.\n"
            "You can configure a scheduler on your instance using dagster.yaml.\n"
            "For more information, see:\n\n"
            "https://docs.dagster.io/deploying/instance/#scheduler"
        )


@click.command(
    name="preview", help="Preview changes that will be performed by `dagster schedule up"
)
@repository_target_argument
def schedule_preview_command(**kwargs):
    return execute_preview_command(kwargs, click.echo)


def execute_preview_command(cli_args, print_fn):
    instance = DagsterInstance.get()
    with get_external_repository_from_kwargs(cli_args, instance) as external_repo:
        check_repo_and_scheduler(external_repo, instance)

        print_changes(external_repo, instance, print_fn, preview=True)


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
        # Not 100% clear what exactly to do here. If we mark this as FAILURE it will
        # prevent the scheduler from ever running the schedule at this timestamp again,
        # which may not be what we want? Thinking of transient Dagster system issues here.
        # User code errors should certainly be marked with FAILURE though.

        # Don't write a tick failure (and keep the schedule from being re-run by the scheduler)
        # if the process is interrupted
        if not isinstance(e, KeyboardInterrupt):
            error_data = serializable_error_info_from_exc_info(sys.exc_info())
            holder.update_with_status(ScheduleTickStatus.FAILURE, error=error_data)
    finally:
        holder.write()


@click.command(name="scheduler", help="Continually launches runs based on active schedules")
def scheduler_command():
    instance = DagsterInstance.get()

    schedules = [
        s for s in instance.all_stored_schedule_state() if s.status == ScheduleStatus.RUNNING
    ]

    for schedule_state in schedules:
        schedule_origin = schedule_state.origin
        schedule_name = schedule_state.name

        # Need to isolate failures loading external artifacts here,
        # log them somewhere... Could be very spammy to see them every time the
        # scheduler runs though?
        repo_location = RepositoryLocation.from_handle(
            location_handle_from_load_target(
                schedule_origin.repository_origin.get_load_target(), instance,
            )
        )

        external_repo = get_external_repository_from_repo_location(
            repo_location, provided_repo_name=None
        )

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

        # Start time is either specified in the definition (to support catchup/automatic backfills)
        # or set to the time when the schedule was started. We may want to distinguish between these
        # two cases in the schedule storage
        #
        # We should checkpoint once we know we've scheduled all runs up to a certain time
        # so that we don't have to start from the beginning and check for every schedule
        start_time_utc = schedule_state.start_time_utc

        # Need to distinguish between a transient failure that should be retried (might
        # as well bubble up and rely on the surrounding infra to retry the whole scheduler command,
        # as though it crashed?) and a user code failure

        # Double-check this still works when start_time_utc matches a cron time exactly
        for schedule_time_utc in croniter_range(
            start_time_utc, get_current_datetime_in_utc(), schedule_state.cron_schedule
        ):
            _schedule_run_at_time(
                instance,
                schedule_state,
                schedule_time_utc,
                repo_location,
                external_repo,
                external_schedule,
                external_pipeline,
            )


def _schedule_run_at_time(
    instance,
    schedule_state,
    schedule_time_utc,
    repo_location,
    external_repo,
    external_schedule,
    external_pipeline,
):
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

        # Rule out the case where the scheduler crashed between creating a run for this time
        # and launching it (Should we log that case?)
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
                # A run already exists and was launched for this time period
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
    # We should pass in the schedule_time_utc here, let partition-less schedules know their
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

    # Enter the run in the DB with the information we have
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


@click.command(
    name="up",
    help="Updates the internal dagster representation of schedules to match the list "
    "of ScheduleDefinitions defined in the repository. Use `dagster schedule up --preview` or "
    "`dagster schedule preview` to preview what changes will be applied. New ScheduleDefinitions "
    "will not start running by default when `up` is called. Use `dagster schedule start` and "
    "`dagster schedule stop` to start and stop a schedule. If a ScheduleDefinition is deleted, the "
    "corresponding running schedule will be stopped and deleted.",
)
@click.option("--preview", help="Preview changes", is_flag=True, default=False)
@repository_target_argument
def schedule_up_command(preview, **kwargs):
    return execute_up_command(preview, kwargs, click.echo)


def execute_up_command(preview, cli_args, print_fn):
    instance = DagsterInstance.get()
    with get_external_repository_from_kwargs(cli_args, instance) as external_repo:
        check_repo_and_scheduler(external_repo, instance)

        print_changes(external_repo, instance, print_fn, preview=preview)
        if preview:
            return

        try:
            instance.reconcile_scheduler_state(external_repo)
        except DagsterInvariantViolationError as ex:
            raise click.UsageError(ex)


@click.command(
    name="list", help="List all schedules that correspond to a repository.",
)
@repository_target_argument
@click.option("--running", help="Filter for running schedules", is_flag=True, default=False)
@click.option("--stopped", help="Filter for stopped schedules", is_flag=True, default=False)
@click.option("--name", help="Only display schedule schedule names", is_flag=True, default=False)
def schedule_list_command(running, stopped, name, **kwargs):
    return execute_list_command(running, stopped, name, kwargs, click.echo)


def execute_list_command(running_filter, stopped_filter, name_filter, cli_args, print_fn):
    instance = DagsterInstance.get()
    with get_external_repository_from_kwargs(cli_args, instance) as external_repo:
        check_repo_and_scheduler(external_repo, instance)

        repository_name = external_repo.name

        if not name_filter:
            title = "Repository {name}".format(name=repository_name)
            print_fn(title)
            print_fn("*" * len(title))

        first = True

        if running_filter:
            schedules = [
                s
                for s in instance.all_stored_schedule_state(external_repo.get_origin_id())
                if s.status == ScheduleStatus.RUNNING
            ]
        elif stopped_filter:
            schedules = [
                s
                for s in instance.all_stored_schedule_state(external_repo.get_origin_id())
                if s.status == ScheduleStatus.STOPPED
            ]
        else:
            schedules = instance.all_stored_schedule_state(external_repo.get_origin_id())

        for schedule_state in schedules:
            # If --name filter is present, only print the schedule name
            if name_filter:
                print_fn(schedule_state.name)
                continue

            flag = "[{status}]".format(status=schedule_state.status.value) if schedule_state else ""
            schedule_title = "Schedule: {name} {flag}".format(name=schedule_state.name, flag=flag)

            if not first:
                print_fn("*" * len(schedule_title))
            first = False

            print_fn(schedule_title)
            print_fn(
                "Cron Schedule: {cron_schedule}".format(cron_schedule=schedule_state.cron_schedule)
            )


def extract_schedule_name(schedule_name):
    if schedule_name and not isinstance(schedule_name, six.string_types):
        if len(schedule_name) == 1:
            return schedule_name[0]
        else:
            check.failed(
                "Can only handle zero or one schedule args. Got {schedule_name}".format(
                    schedule_name=repr(schedule_name)
                )
            )


@click.command(name="start", help="Start an existing schedule")
@click.argument("schedule_name", nargs=-1)  # , required=True)
@click.option("--start-all", help="start all schedules", is_flag=True, default=False)
@repository_target_argument
def schedule_start_command(schedule_name, start_all, **kwargs):
    schedule_name = extract_schedule_name(schedule_name)
    if schedule_name is None and start_all is False:
        print(  # pylint: disable=print-call
            "Noop: dagster schedule start was called without any arguments specifying which "
            "schedules to start. Pass a schedule name or the --start-all flag to start schedules."
        )
        return
    return execute_start_command(schedule_name, start_all, kwargs, click.echo)


def execute_start_command(schedule_name, all_flag, cli_args, print_fn):
    instance = DagsterInstance.get()
    with get_external_repository_from_kwargs(cli_args, instance) as external_repo:
        check_repo_and_scheduler(external_repo, instance)

        repository_name = external_repo.name

        if all_flag:
            for external_schedule in external_repo.get_external_schedules():
                try:
                    instance.start_schedule_and_update_storage_state(external_schedule)
                except DagsterInvariantViolationError as ex:
                    raise click.UsageError(ex)

            print_fn(
                "Started all schedules for repository {repository_name}".format(
                    repository_name=repository_name
                )
            )
        else:
            try:

                instance.start_schedule_and_update_storage_state(
                    external_repo.get_external_schedule(schedule_name)
                )
            except DagsterInvariantViolationError as ex:
                raise click.UsageError(ex)

            print_fn("Started schedule {schedule_name}".format(schedule_name=schedule_name))


@click.command(name="stop", help="Stop an existing schedule")
@click.argument("schedule_name", nargs=-1)
@repository_target_argument
def schedule_stop_command(schedule_name, **kwargs):
    schedule_name = extract_schedule_name(schedule_name)
    return execute_stop_command(schedule_name, kwargs, click.echo)


def execute_stop_command(schedule_name, cli_args, print_fn, instance=None):
    instance = DagsterInstance.get()
    with get_external_repository_from_kwargs(cli_args, instance) as external_repo:
        check_repo_and_scheduler(external_repo, instance)

        try:
            instance.stop_schedule_and_update_storage_state(
                external_repo.get_external_schedule(schedule_name).get_origin_id()
            )
        except DagsterInvariantViolationError as ex:
            raise click.UsageError(ex)

        print_fn("Stopped schedule {schedule_name}".format(schedule_name=schedule_name))


@click.command(name="logs", help="Get logs for a schedule")
@click.argument("schedule_name", nargs=-1)
@repository_target_argument
def schedule_logs_command(schedule_name, **kwargs):
    schedule_name = extract_schedule_name(schedule_name)
    if schedule_name is None:
        print(  # pylint: disable=print-call
            "Noop: dagster schedule logs was called without any arguments specifying which "
            "schedules to retrieve logs for. Pass a schedule name"
        )
        return
    return execute_logs_command(schedule_name, kwargs, click.echo)


def execute_logs_command(schedule_name, cli_args, print_fn, instance=None):
    instance = DagsterInstance.get()
    with get_external_repository_from_kwargs(cli_args, instance) as external_repo:
        check_repo_and_scheduler(external_repo, instance)
        logs_path = os.path.join(
            instance.logs_path_for_schedule(
                external_repo.get_external_schedule(schedule_name).get_origin_id()
            )
        )
        print_fn(logs_path)


@click.command(name="restart", help="Restart a running schedule")
@click.argument("schedule_name", nargs=-1)
@click.option(
    "--restart-all-running",
    help="restart previously running schedules",
    is_flag=True,
    default=False,
)
@repository_target_argument
def schedule_restart_command(schedule_name, restart_all_running, **kwargs):
    schedule_name = extract_schedule_name(schedule_name)
    return execute_restart_command(schedule_name, restart_all_running, kwargs, click.echo)


def execute_restart_command(schedule_name, all_running_flag, cli_args, print_fn):
    instance = DagsterInstance.get()
    with get_external_repository_from_kwargs(cli_args, instance) as external_repo:
        check_repo_and_scheduler(external_repo, instance)

        repository_name = external_repo.name

        if all_running_flag:
            for schedule_state in instance.all_stored_schedule_state(external_repo.get_origin_id()):
                if schedule_state.status == ScheduleStatus.RUNNING:
                    try:
                        external_schedule = external_repo.get_external_schedule(schedule_state.name)
                        instance.stop_schedule_and_update_storage_state(
                            schedule_state.schedule_origin_id
                        )
                        instance.start_schedule_and_update_storage_state(external_schedule)
                    except DagsterInvariantViolationError as ex:
                        raise click.UsageError(ex)

            print_fn(
                "Restarted all running schedules for repository {name}".format(name=repository_name)
            )
        else:
            external_schedule = external_repo.get_external_schedule(schedule_name)
            schedule_state = instance.get_schedule_state(external_schedule.get_origin_id())
            if schedule_state.status != ScheduleStatus.RUNNING:
                click.UsageError(
                    "Cannot restart a schedule {name} because is not currently running".format(
                        name=schedule_state.name
                    )
                )

            try:
                instance.stop_schedule_and_update_storage_state(schedule_state.schedule_origin_id)
                instance.start_schedule_and_update_storage_state(external_schedule)
            except DagsterInvariantViolationError as ex:
                raise click.UsageError(ex)

            print_fn("Restarted schedule {schedule_name}".format(schedule_name=schedule_name))


@click.command(name="wipe", help="Deletes all schedules and schedule cron jobs.")
@repository_target_argument
def schedule_wipe_command(**kwargs):
    return execute_wipe_command(kwargs, click.echo)


def execute_wipe_command(cli_args, print_fn):
    instance = DagsterInstance.get()
    with get_external_repository_from_kwargs(cli_args, instance) as external_repo:
        check_repo_and_scheduler(external_repo, instance)

        confirmation = click.prompt(
            "Are you sure you want to delete all schedules and schedule cron jobs? Type DELETE"
        )
        if confirmation == "DELETE":
            instance.wipe_all_schedules()
            print_fn("Wiped all schedules and schedule cron jobs")
        else:
            click.echo("Exiting without deleting all schedules and schedule cron jobs")


@click.command(name="debug", help="Debug information about the scheduler")
def schedule_debug_command():
    return execute_debug_command(click.echo)


def execute_debug_command(print_fn):
    instance = DagsterInstance.get()

    debug_info = instance.scheduler_debug_info()

    output = ""

    errors = debug_info.errors
    if len(errors):
        title = "Errors (Run `dagster schedule up` to resolve)"
        output += "\n{title}\n{sep}\n{info}\n\n".format(
            title=title, sep="=" * len(title), info="\n".join(debug_info.errors),
        )

    title = "Scheduler Configuration"
    output += "{title}\n{sep}\n{info}\n".format(
        title=title, sep="=" * len(title), info=debug_info.scheduler_config_info,
    )

    title = "Scheduler Info"
    output += "{title}\n{sep}\n{info}\n".format(
        title=title, sep="=" * len(title), info=debug_info.scheduler_info
    )

    title = "Scheduler Storage Info"
    output += "\n{title}\n{sep}\n{info}\n".format(
        title=title, sep="=" * len(title), info="\n".join(debug_info.schedule_storage),
    )

    print_fn(output)


schedule_cli = create_schedule_cli_group()
