import os
import sys

import click

from dagster import __version__
from dagster.instigator.cli.dequeuer import create_dequeuer_cli_group


def create_dagster_instigator_cli():
    commands = {
        "dequeuer": create_dequeuer_cli_group(),
    }

    @click.group(commands=commands)
    @click.version_option(version=__version__)
    def group():
        "CLI tools for working with dagster run instigation."

    return group


cli = create_dagster_instigator_cli()


def main():
    cli(obj={})  # pylint:disable=E1123
