from __future__ import print_function

import string

import pytest

from dagster.cli.pipeline import execute_backfill_command
from dagster.utils import merge_dicts

from .test_cli_commands import backfill_command_contexts


def run_test_backfill(execution_args, instance, expected_count=None, error_message=None):
    if error_message:
        with pytest.raises(Exception):
            execute_backfill_command(execution_args, print, instance)
    elif expected_count:
        execute_backfill_command(execution_args, print, instance)
        assert instance.get_runs_count() == expected_count


@pytest.mark.parametrize('backfill_args_context', backfill_command_contexts())
def test_backfill_no_pipeline(backfill_args_context):
    with backfill_args_context as (cli_args, instance):
        args = merge_dicts(cli_args, {'pipeline': 'nonexistent'})
        run_test_backfill(args, instance, error_message='No pipeline found')


@pytest.mark.parametrize('backfill_args_context', backfill_command_contexts())
def test_backfill_no_partition_sets(backfill_args_context):
    with backfill_args_context as (cli_args, instance):
        args = merge_dicts(cli_args, {'pipeline': 'foo'})
        run_test_backfill(args, instance, error_message='No partition sets found')


@pytest.mark.parametrize('backfill_args_context', backfill_command_contexts())
def test_backfill_no_named_partition_set(backfill_args_context):
    with backfill_args_context as (cli_args, instance):
        args = merge_dicts(cli_args, {'pipeline': 'baz', 'partition_set': 'nonexistent'})
        run_test_backfill(args, instance, error_message='No partition set found')


@pytest.mark.parametrize('backfill_args_context', backfill_command_contexts())
def test_backfill_launch(backfill_args_context):
    with backfill_args_context as (cli_args, instance):
        args = merge_dicts(cli_args, {'pipeline': 'baz', 'partition_set': 'baz_partitions'})
        run_test_backfill(args, instance, expected_count=len(string.digits))


@pytest.mark.parametrize('backfill_args_context', backfill_command_contexts())
def test_backfill_partition_range(backfill_args_context):
    with backfill_args_context as (cli_args, instance):
        args = merge_dicts(
            cli_args, {'pipeline': 'baz', 'partition_set': 'baz_partitions', 'from': '7'}
        )
        run_test_backfill(args, instance, expected_count=3)

        args = merge_dicts(
            cli_args, {'pipeline': 'baz', 'partition_set': 'baz_partitions', 'to': '2'}
        )
        run_test_backfill(args, instance, expected_count=6)  # 3 more runs

        args = merge_dicts(
            cli_args, {'pipeline': 'baz', 'partition_set': 'baz_partitions', 'from': '2', 'to': '5'}
        )
        run_test_backfill(args, instance, expected_count=10)  # 4 more runs


@pytest.mark.parametrize('backfill_args_context', backfill_command_contexts())
def test_backfill_partition_enum(backfill_args_context):
    with backfill_args_context as (cli_args, instance):
        args = merge_dicts(
            cli_args, {'pipeline': 'baz', 'partition_set': 'baz_partitions', 'partitions': '2,9,0'}
        )
        run_test_backfill(args, instance, expected_count=3)


@pytest.mark.parametrize('backfill_args_context', backfill_command_contexts())
def test_backfill_tags_pipeline(backfill_args_context):
    with backfill_args_context as (cli_args, instance):
        args = merge_dicts(
            cli_args,
            {
                'partition_set': 'baz_partitions',
                'partitions': '2',
                'tags': '{ "foo": "bar" }',
                'pipeline': 'baz',
            },
        )
        execute_backfill_command(args, print, instance)
        runs = instance.get_runs()
        assert len(runs) == 1
        run = runs[0]
        assert len(run.tags) >= 1
        assert run.tags.get('foo') == 'bar'
