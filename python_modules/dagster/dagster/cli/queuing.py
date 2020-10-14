import click

from dagster import DagsterInstance
from dagster.queuing.dequeuer import DagsterDequeuer, execute_dagster_dequeuer


def create_queuing_cli_group():
    group = click.Group(name="queuing")
    group.add_command(dequeuer_run_command)
    return group


@click.command(
    name="run", help="Poll for queued runs and launch them",
)
@click.option("--interval", help="How frequently to check for runs to launch", default=2)
@click.option(
    "--max-concurrent-runs", help="Max number of runs that should be executing at once", default=10,
)
def dequeuer_run_command(interval, max_concurrent_runs):
    dequeuer = DagsterDequeuer(DagsterInstance.get(), max_concurrent_runs=max_concurrent_runs)
    execute_dagster_dequeuer(dequeuer, interval=interval)


queuing_cli = create_queuing_cli_group()
