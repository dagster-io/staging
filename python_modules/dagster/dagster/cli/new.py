import click
import os

from dagster.templates import generate_simple_project


@click.command(name="new")
@click.argument("project_location", type=click.Path())
@click.option(
    "--schedules", is_flag=True, help="If set, then setup for Dagster schedules will be included.",
)
@click.option(
    "--sensors", is_flag=True, help="If set, then setup for Dagster sensors will be included.",
)
def new_command(project_location: str, schedules: bool, sensors: bool):
    """
    Creates a new Dagster project skeleton.

    PROJECT_LOCATION: Location of the new Dagster project in your filesystem
    """
    click.echo(f"Creating a new Dagster project in {project_location}...")
    try:
        generate_simple_project(project_location)
    except FileExistsError:
        if os.path.exists(project_location):
            click.echo(
                f"The path {project_location} already exists. Please delete the contents of this path or choose another project location."
            )
            return

    click.echo(f"Done.")


new_cli = new_command
