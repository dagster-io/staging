import importlib
import click

from collections import namedtuple
from dagster import execute_pipeline, DagsterInstance, check
from dagster.cli.workspace.cli_target import (
    get_external_repository_from_kwargs,
    repository_target_argument,
)
from lakehouse.asset import Asset
from lakehouse.house import Lakehouse


def load_assets(module_name):
    module = importlib.import_module(module_name)
    attrs = [getattr(module, attr_name) for attr_name in dir(module)]
    assets = [attr for attr in attrs if isinstance(attr, Asset)]
    return assets


def load_lakehouses(module_name):
    module = importlib.import_module(module_name)
    attrs = [getattr(module, attr_name) for attr_name in dir(module)]
    lakehouses = [attr for attr in attrs if isinstance(attr, Lakehouse)]
    return lakehouses


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


def _sorted_quoted(strings):
    return '[' + ', '.join(["'{}'".format(s) for s in sorted(list(strings))]) + ']'


def get_internal_repository_from_kwargs(kwargs, instance):
    check.inst_param(instance, 'instance', DagsterInstance)
    repo_location = get_repository_location_from_kwargs(kwargs, instance)

    repo_dict = repo_location.get_repositories()

    provided_repo_name = kwargs.get('repository')

    check.invariant(repo_dict, 'There should be at least one repo.')

    # no name provided and there is only one repo. Automatically return
    if provided_repo_name is None and len(repo_dict) == 1:
        return next(iter(repo_dict.values()))

    if provided_repo_name is None:
        raise click.UsageError(
            (
                'Must provide --repository as there are more than one repositories '
                'in {location}. Options are: {repos}.'
            ).format(location=repo_location.name, repos=_sorted_quoted(repo_dict.keys()))
        )

    if not repo_location.has_repository(provided_repo_name):
        raise click.UsageError(
            (
                'Repository "{provided_repo_name}" not found in location "{location_name}". '
                'Found {found_names} instead.'
            ).format(
                provided_repo_name=provided_repo_name,
                location_name=repo_location.name,
                found_names=_sorted_quoted(repo_dict.keys()),
            )
        )

    return repo_location.get_repository(provided_repo_name)


@click.command()
@click.option('--module', type=click.STRING, required=True)
@click.option('--mode', type=click.STRING, default='default')
@click.option('--assets', type=click.STRING, default='*')
def update_cli(module, mode, assets):
    lakehouses = load_lakehouses(module)
    check.invariant(len(lakehouses) == 1, 'There should only be one lakehouse')
    lakehouse = lakehouses[0]
    asset_defs = lakehouse.query_assets(assets)

    execute_pipeline(
        lakehouse.build_pipeline_definition('update ' + assets, asset_defs), mode=mode,
    )


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
