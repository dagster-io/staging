import importlib

import click
from lakehouse.house import Lakehouse

from dagster import check, execute_pipeline


def load_lakehouses(module_name):
    module = importlib.import_module(module_name)
    attrs = [getattr(module, attr_name) for attr_name in dir(module)]
    lakehouses = [attr for attr in attrs if isinstance(attr, Lakehouse)]
    return lakehouses


@click.command(help="Builds and executes a pipeline that updates the given set of assets.")
@click.option(
    "--module",
    type=click.STRING,
    required=True,
    help="A module containing a single Lakehouse definition",
)
@click.option(
    "--mode", type=click.STRING, default="default", help="The mode to launch the pipeline in."
)
@click.option(
    "--assets",
    type=click.STRING,
    default="*",
    help="An expression to select the set of assets to update.",
)
def update_cli(module, mode, assets):
    lakehouses = load_lakehouses(module)
    check.invariant(len(lakehouses) == 1, "There should only be one lakehouse")
    lakehouse = lakehouses[0]
    asset_defs = lakehouse.query_assets(assets)

    execute_pipeline(
        lakehouse.build_pipeline_definition("update " + assets, asset_defs), mode=mode,
    )


def create_lakehouse_cli():
    commands = {
        "update": update_cli,
    }

    @click.group(commands=commands)
    def group():
        "CLI tools for working with lakehouse."

    return group


cli = create_lakehouse_cli()


def main():
    cli(obj={})  # pylint:disable=E1123
