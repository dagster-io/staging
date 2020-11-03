import os
import sys

import click
from dagster import __version__
from dagster.core.instance import DagsterInstance
from dagster.daemon import DagsterDaemonController


@click.command(
    name="run", help="Run any daemons configured on the DagsterInstance.",
)
def run_command():
    with DagsterInstance.get() as instance:
        controller = DagsterDaemonController(instance)
        controller.run()


def create_dagster_daemon_cli():
    commands = {
        "run": run_command,
    }

    @click.group(commands=commands)
    @click.version_option(version=__version__)
    def group():
        "CLI tools for working with the dagster daemon process."

    return group


cli = create_dagster_daemon_cli()


def main():
    cli(obj={})  # pylint:disable=E1123
