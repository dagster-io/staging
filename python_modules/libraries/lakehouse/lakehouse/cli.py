import importlib
import click

from collections import namedtuple
from dagster import execute_pipeline, DagsterInstance
from dagster.cli.workspace.cli_target import (
    get_external_repository_from_kwargs,
    repository_target_argument,
)


def load_assets(module_name):
    module = importlib.import_module(module_name)
    return getattr(module, 'assets')


def load_lakehouse(module_name):
    module = importlib.import_module(module_name)
    return getattr(module, 'lakehouse')


def print_table(rows, spacing=7):
    if not rows:
        return

    col_max_lens = [0] * len(rows[0])
    for row in rows:
        for i, el in enumerate(row):
            col_max_lens[i] = max(col_max_lens[i], len(el))

    template = ''.join(['{:<' + str(col_max_len + spacing) + '}' for col_max_len in col_max_lens])
    for row in rows:
        print(template.format(*row))


AssetStatus = namedtuple('AssetStatus', 'asset def_hash materialization_path')


def collect_asset_statuses(assets):
    asset_definition_hashes = compute_asset_definition_hashes(assets)

    materializations_tracker = MaterializationsTracker()
    materialization_paths_by_hash = materializations_tracker.get_paths_for_hashes(
        [def_hash for _, def_hash in asset_definition_hashes]
    )

    return [
        AssetStatus(asset, def_hash, materialization_paths_by_hash.get(def_hash))
        for asset, def_hash in asset_definition_hashes
    ]


@click.command()
@click.option('--module', type=click.STRING)
def status_cli(module):
    assets = load_assets(module)
    asset_statuses = collect_asset_statuses(assets)

    print_table(
        [('ASSET KEY', 'DEFINITION HASH', 'MATERIALIZATION PATH')]
        + [
            (
                status.asset.path[0],
                status.def_hash[0:6],
                ('/'.join(status.materialization_path or [])) or '<missing>',
            )
            for status in asset_statuses
            if status.asset.computation
        ]
    )


@click.command()
@click.option('--module', type=click.STRING)
def fill_cli(module):
    assets = load_assets(module)
    lakehouse = load_lakehouse(module)
    asset_statuses = collect_asset_statuses(assets)
    to_fill = [
        status
        for status in asset_statuses
        if not status.materialization_path and status.asset.computation
    ]

    execute_pipeline(
        lakehouse.build_pipeline_definition(
            'fill_assets_pipeline', [asset_status.asset for asset_status in to_fill]
        ),
        mode='dev',
    )

    materializations_tracker = MaterializationsTracker()
    materializations_tracker.save_paths_for_hashes(
        {status.def_hash: status.asset.path for status in to_fill}
    )


@click.command()
@repository_target_argument
def update_cli(**kwargs):
    instance = DagsterInstance.get()

    repo = get_external_repository_from_kwargs(kwargs, instance)
    lakehouse_def = repo.get_lakehouse_def()


def create_lakehouse_cli():
    commands = {
        'status': status_cli,
        'fill': fill_cli,
        'update': update_cli,
    }

    @click.group(commands=commands)
    def group():
        'CLI tools for working with lakehouse.'

    return group


cli = create_lakehouse_cli()


def main():
    cli(obj={})  # pylint:disable=E1123
