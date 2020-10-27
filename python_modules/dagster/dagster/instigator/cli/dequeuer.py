import click

from dagster import DagsterInstance
from dagster.instigator.default_run_dequeuer import DefaultRunDequeuer


def create_dequeuer_cli_group():
    group = click.Group(name="dequeuer")
    group.add_command(dequeuer_run_command)
    return group


@click.command(
    name="run", help="Poll for queued runs and launch them",
)
@click.option(
    "--interval-seconds", help="How long to wait (seconds) between polls for runs", default=2
)
@click.option(
    "--max-concurrent-runs", help="Max number of runs that should be executing at once", default=10,
)
def dequeuer_run_command(interval_seconds, max_concurrent_runs):
    dequeuer = DefaultRunDequeuer(DagsterInstance.get(), max_concurrent_runs=max_concurrent_runs)
    click.echo("Starting run dequeuer")
    dequeuer.run(interval_seconds=interval_seconds)
