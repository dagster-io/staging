from __future__ import print_function

import re

import pytest
from click.testing import CliRunner

from dagster.cli.pipeline import execute_launch_command, pipeline_launch_command
from dagster.core.test_utils import mocked_instance
from dagster.utils import file_relative_path

from .test_cli_commands import valid_pipeline_args, valid_pipeline_target_cli_args


def run_launch(kwargs, instance, expected_count=None):
    run = execute_launch_command(instance, kwargs)
    assert run
    if expected_count:
        assert instance.get_runs_count() == expected_count
    instance.run_launcher.join()


def run_launch_cli(execution_args, expected_count=None):
    runner = CliRunner()
    with mocked_instance() as instance:
        result = runner.invoke(pipeline_launch_command, execution_args)
        assert result.exit_code == 0, result.stdout
        if expected_count:
            assert instance.get_runs_count() == expected_count


@pytest.mark.parametrize('gen_pipeline_args', valid_pipeline_args())
def test_launch_pipeline(gen_pipeline_args):
    with gen_pipeline_args as (cli_args, uses_legacy_repository_yaml_format, instance):
        if uses_legacy_repository_yaml_format:
            with pytest.warns(
                UserWarning,
                match=re.escape(
                    'You are using the legacy repository yaml format. Please update your file '
                ),
            ):
                run_launch(cli_args, instance, expected_count=1)
        else:
            run_launch(cli_args, instance, expected_count=1)


@pytest.mark.parametrize('pipeline_cli_args', valid_pipeline_target_cli_args())
def test_launch_pipeline_cli(pipeline_cli_args):
    cli_args, uses_legacy_repository_yaml_format = pipeline_cli_args
    if uses_legacy_repository_yaml_format:
        with pytest.warns(
            UserWarning,
            match=re.escape(
                'You are using the legacy repository yaml format. Please update your file '
            ),
        ):
            run_launch_cli(cli_args, expected_count=1)
    else:
        run_launch_cli(cli_args, expected_count=1)


def test_launch_subset_pipeline():
    runner = CliRunner()
    # single clause, solid name
    with mocked_instance() as instance:
        result = runner.invoke(
            pipeline_launch_command,
            [
                '-f',
                file_relative_path(__file__, 'test_cli_commands.py'),
                '-a',
                'foo_pipeline',
                '--solid-selection',
                'do_something',
            ],
        )
        assert result.exit_code == 0
        runs = instance.get_runs()
        assert len(runs) == 1
        run = runs[0]
        assert run.solid_selection == ['do_something']
        assert run.solids_to_execute == {'do_something'}

    # single clause, DSL query
    with mocked_instance() as instance:
        result = runner.invoke(
            pipeline_launch_command,
            [
                '-f',
                file_relative_path(__file__, 'test_cli_commands.py'),
                '-a',
                'foo_pipeline',
                '--solid-selection',
                '*do_something+',
            ],
        )
        assert result.exit_code == 0
        runs = instance.get_runs()
        assert len(runs) == 1
        run = runs[0]
        assert run.solid_selection == ['*do_something+']
        assert run.solids_to_execute == {'do_something', 'do_input'}

    # multiple clauses, DSL query and solid name
    with mocked_instance() as instance:
        result = runner.invoke(
            pipeline_launch_command,
            [
                '-f',
                file_relative_path(__file__, 'test_cli_commands.py'),
                '-a',
                'foo_pipeline',
                '--solid-selection',
                '*do_something+,do_input',
            ],
        )
        assert result.exit_code == 0
        runs = instance.get_runs()
        assert len(runs) == 1
        run = runs[0]
        assert set(run.solid_selection) == set(['*do_something+', 'do_input'])
        assert run.solids_to_execute == {'do_something', 'do_input'}

    # invalid value
    with mocked_instance() as instance:
        result = runner.invoke(
            pipeline_launch_command,
            [
                '-f',
                file_relative_path(__file__, 'test_cli_commands.py'),
                '-a',
                'foo_pipeline',
                '--solid-selection',
                'a, b',
            ],
        )
        assert result.exit_code == 1
        assert 'No qualified solids to execute found for solid_selection' in str(result.exception)
