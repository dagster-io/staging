from __future__ import print_function

import click
import six

from dagster import DagsterInvariantViolationError, check
from dagster.cli.workspace.cli_target import (
    get_external_repository_from_kwargs,
    repository_target_argument,
)
from dagster.core.instance import DagsterInstance
from dagster.core.definitions.job import JobType
from dagster.core.scheduler.job import JobState, JobStatus


def create_sensor_cli_group():
    group = click.Group(name="sensor")
    group.add_command(sensor_list_command)
    group.add_command(sensor_up_command)
    group.add_command(sensor_start_command)
    group.add_command(sensor_stop_command)
    return group


def print_changes(external_repository, instance, print_fn=print, preview=False):
    sensor_states = instance.all_stored_job_state(
        external_repository.get_origin_id(), JobType.SENSOR
    )
    external_sensors = external_repository.get_external_sensors()
    external_sensors_dict = {s.get_origin_id(): s for s in external_sensors}
    sensor_states_dict = {s.job_origin_id: s for s in sensor_states}

    external_sensor_origin_ids = set(external_sensors_dict.keys())
    sensor_state_ids = set(sensor_states_dict.keys())

    added_sensors = external_sensor_origin_ids - sensor_state_ids
    removed_sensors = sensor_state_ids - external_sensor_origin_ids

    if not added_sensors and not removed_sensors:
        if preview:
            print_fn(click.style("No planned changes to sensors.", fg="magenta", bold=True))
            print_fn("{num} sensors will remain unchanged".format(num=len(external_sensors)))
        else:
            print_fn(click.style("No changes to sensors.", fg="magenta", bold=True))
            print_fn("{num} sensors unchanged".format(num=len(external_sensors)))
        return

    print_fn(
        click.style("Planned Job Changes:" if preview else "Changes:", fg="magenta", bold=True)
    )

    for sensor_origin_id in added_sensors:
        print_fn(
            click.style(
                "  + {name} (add) [{id}]".format(
                    name=external_sensors_dict[sensor_origin_id].name, id=sensor_origin_id
                ),
                fg="green",
            )
        )

    for sensor_origin_id in removed_sensors:
        print_fn(
            click.style(
                "  + {name} (delete) [{id}]".format(
                    name=external_sensors_dict[sensor_origin_id].name, id=sensor_origin_id
                ),
                fg="red",
            )
        )


def extract_sensor_name(sensor_name):
    if sensor_name and not isinstance(sensor_name, six.string_types):
        if len(sensor_name) == 1:
            return sensor_name[0]
        else:
            check.failed(
                "Can only handle zero or one sensor args. Got {sensor_name}".format(
                    sensor_name=repr(sensor_name)
                )
            )


@click.command(
    name="list", help="List all sensors that correspond to a repository.",
)
@repository_target_argument
@click.option("--running", help="Filter for running sensors", is_flag=True, default=False)
@click.option("--stopped", help="Filter for stopped sensors", is_flag=True, default=False)
@click.option("--name", help="Only display sensor sensor names", is_flag=True, default=False)
def sensor_list_command(running, stopped, name, **kwargs):
    return execute_list_command(running, stopped, name, kwargs, click.echo)


def execute_list_command(running_filter, stopped_filter, name_filter, cli_args, print_fn):
    with DagsterInstance.get() as instance:
        with get_external_repository_from_kwargs(cli_args) as external_repo:
            repository_name = external_repo.name

            if not name_filter:
                title = "Repository {name}".format(name=repository_name)
                print_fn(title)
                print_fn("*" * len(title))

            first = True

            if running_filter:
                sensors = [
                    s
                    for s in instance.all_stored_job_state(
                        external_repo.get_origin_id(), JobType.SENSOR
                    )
                    if s.status == JobStatus.RUNNING
                ]
            elif stopped_filter:
                sensors = [
                    s
                    for s in instance.all_stored_job_state(
                        external_repo.get_origin_id(), JobType.SENSOR
                    )
                    if s.status == JobStatus.STOPPED
                ]
            else:
                sensors = instance.all_stored_job_state(
                    external_repo.get_origin_id(), JobType.SENSOR
                )

            for sensor_state in sensors:
                # If --name filter is present, only print the sensor name
                if name_filter:
                    print_fn(sensor_state.name)
                    continue

                flag = "[{status}]".format(status=sensor_state.status.value) if sensor_state else ""
                sensor_title = "Job: {name} {flag}".format(name=sensor_state.name, flag=flag)

                if not first:
                    print_fn("*" * len(sensor_title))
                first = False

                print_fn(sensor_title)


@click.command(
    name="up",
    help="Updates the internal dagster representation of sensors to match the list "
    "of JobDefinitions defined in the repository. Use `dagster sensor up --preview` or "
    "`dagster sensor preview` to preview what changes will be applied. New JobDefinitions "
    "will not start running by default when `up` is called. Use `dagster sensor start` and "
    "`dagster sensor stop` to start and stop a sensor. If a JobDefinition is deleted, the "
    "corresponding running sensor will be stopped and deleted.",
)
@click.option("--preview", help="Preview changes", is_flag=True, default=False)
@repository_target_argument
def sensor_up_command(preview, **kwargs):
    return execute_up_command(preview, kwargs, click.echo)


def execute_up_command(preview, cli_args, print_fn):
    with DagsterInstance.get() as instance:
        with get_external_repository_from_kwargs(cli_args) as external_repo:
            print_changes(external_repo, instance, print_fn, preview=preview)
            if preview:
                return

            try:
                for external_sensor in external_repo.get_external_sensors():
                    existing_job_state = instance.get_job_state(external_sensor.get_origin_id())
                    if not existing_job_state:
                        job_state = JobState(
                            external_sensor.get_origin(), JobType.SENSOR, JobStatus.STOPPED
                        )
                        instance.add_job_state(job_state)

                external_sensor_origin_ids = {
                    s.get_origin_id() for s in external_repo.get_external_sensors()
                }
                existing_sensor_origin_ids = set(
                    [
                        job.job_origin_id
                        for job in instance.all_stored_job_state(
                            external_repo.get_origin_id(), JobType.SENSOR
                        )
                    ]
                )

                sensor_origin_ids_to_delete = (
                    existing_sensor_origin_ids - external_sensor_origin_ids
                )
                for sensor_origin_id in sensor_origin_ids_to_delete:
                    instance.delete_job_state(sensor_origin_id)

            except DagsterInvariantViolationError as ex:
                raise click.UsageError(ex)


@click.command(name="start", help="Start an existing sensor")
@click.argument("sensor_name", nargs=-1)  # , required=True)
@click.option("--start-all", help="start all sensors", is_flag=True, default=False)
@repository_target_argument
def sensor_start_command(sensor_name, start_all, **kwargs):
    sensor_name = extract_sensor_name(sensor_name)
    if sensor_name is None and start_all is False:
        print(  # pylint: disable=print-call
            "Noop: dagster sensor start was called without any arguments specifying which "
            "sensors to start. Pass a sensor name or the --start-all flag to start sensors."
        )
        return
    return execute_start_command(sensor_name, start_all, kwargs, click.echo)


def execute_start_command(sensor_name, all_flag, cli_args, print_fn):
    with DagsterInstance.get() as instance:
        with get_external_repository_from_kwargs(cli_args) as external_repo:
            repository_name = external_repo.name

            def _start_sensor(external_sensor):
                job_state = JobState(
                    external_sensor.get_origin(), JobType.SENSOR, JobStatus.RUNNING
                )
                existing_job_state = instance.get_job_state(external_sensor.get_origin_id())
                if existing_job_state:
                    instance.update_job_state(job_state)
                else:
                    instance.add_job_state(job_state)

            if all_flag:
                try:
                    for external_sensor in external_repo.get_external_sensors():
                        _start_sensor(external_sensor)
                    print_fn(
                        "Started all sensors for repository {repository_name}".format(
                            repository_name=repository_name
                        )
                    )
                except DagsterInvariantViolationError as ex:
                    raise click.UsageError(ex)
            else:
                try:
                    external_sensor = external_repo.get_external_sensor(sensor_name)
                    _start_sensor(external_sensor)
                except DagsterInvariantViolationError as ex:
                    raise click.UsageError(ex)

                print_fn("Started sensor {sensor_name}".format(sensor_name=sensor_name))


@click.command(name="stop", help="Stop an existing sensor")
@click.argument("sensor_name", nargs=-1)
@repository_target_argument
def sensor_stop_command(sensor_name, **kwargs):
    sensor_name = extract_sensor_name(sensor_name)
    return execute_stop_command(sensor_name, kwargs, click.echo)


def execute_stop_command(sensor_name, cli_args, print_fn, instance=None):
    with DagsterInstance.get() as instance:
        with get_external_repository_from_kwargs(cli_args) as external_repo:
            try:
                external_sensor = external_repo.get_external_sensor(sensor_name)
                job_state = JobState(
                    external_sensor.get_origin(), JobType.SENSOR, JobStatus.STOPPED
                )
                existing_job_state = instance.get_job_state(external_sensor.get_origin_id())
                if existing_job_state:
                    instance.update_job_state(job_state)
                else:
                    instance.add_job_state(job_state)
            except DagsterInvariantViolationError as ex:
                raise click.UsageError(ex)

            print_fn("Stopped sensor {sensor_name}".format(sensor_name=sensor_name))


sensor_cli = create_sensor_cli_group()
