from __future__ import print_function

import os
import re
import string
from contextlib import contextmanager

import pytest
import yaml
from click.testing import CliRunner

from dagster import (
    PartitionSetDefinition,
    ScheduleDefinition,
    lambda_solid,
    pipeline,
    repository,
    seven,
    solid,
)
from dagster.cli.pipeline import (
    execute_backfill_command,
    execute_launch_command,
    execute_scaffold_command,
    pipeline_backfill_command,
    pipeline_launch_command,
    pipeline_scaffold_command,
)
from dagster.cli.run import run_list_command, run_wipe_command
from dagster.core.instance import DagsterInstance
from dagster.core.test_utils import environ, mocked_instance
from dagster.grpc.server import GrpcServerProcess
from dagster.grpc.types import LoadableTargetOrigin
from dagster.utils import file_relative_path, merge_dicts


def no_print(_):
    return None


@lambda_solid
def do_something():
    return 1


@lambda_solid
def do_input(x):
    return x


@pipeline(name='foo')
def foo_pipeline():
    do_input(do_something())


def define_foo_pipeline():
    return foo_pipeline


@pipeline(name='baz', description='Not much tbh')
def baz_pipeline():
    do_input()


def define_bar_schedules():
    return {
        'foo_schedule': ScheduleDefinition(
            "foo_schedule", cron_schedule="* * * * *", pipeline_name="test_pipeline", run_config={},
        ),
    }


def define_baz_partitions():
    return {
        'baz_partitions': PartitionSetDefinition(
            name='baz_partitions',
            pipeline_name='baz',
            partition_fn=lambda: string.digits,
            run_config_fn_for_partition=lambda partition: {
                'solids': {'do_input': {'inputs': {'x': {'value': partition.value}}}}
            },
        )
    }


@repository
def bar():
    return {
        'pipelines': {'foo': foo_pipeline, 'baz': baz_pipeline},
        'schedules': define_bar_schedules(),
        'partition_sets': define_baz_partitions(),
    }


@solid
def spew(context):
    context.log.info('HELLO WORLD')


@solid
def fail(context):
    raise Exception('I AM SUPPOSED TO FAIL')


@pipeline
def stdout_pipeline():
    spew()


@pipeline
def stderr_pipeline():
    fail()


@contextmanager
def managed_grpc_instance():
    with mocked_instance(overrides={"opt_in": {"local_servers": True}}) as instance:
        yield instance


@contextmanager
def args_with_instance(gen_instance, *args):
    with gen_instance as instance:
        yield args + (instance,)


def args_with_default_instance(*args):
    return args_with_instance(mocked_instance(), *args)


def args_with_managed_grpc_instance(*args):
    return args_with_instance(managed_grpc_instance(), *args)


@contextmanager
def grpc_server_bar_kwargs(pipeline_name=None):
    with GrpcServerProcess(
        loadable_target_origin=LoadableTargetOrigin(
            python_file=file_relative_path(__file__, 'test_cli_commands.py'), attribute='bar'
        ),
    ).create_ephemeral_client() as client:
        args = {'grpc_host': client.host}
        if pipeline_name:
            args['pipeline'] = 'foo'
        if client.port:
            args['grpc_port'] = client.port
        if client.socket:
            args['grpc_socket'] = client.socket
        with mocked_instance() as instance:
            yield args, False, instance


@contextmanager
def grpc_server_bar_cli_args():
    with GrpcServerProcess(
        loadable_target_origin=LoadableTargetOrigin(
            python_file=file_relative_path(__file__, 'test_cli_commands.py'), attribute='bar'
        ),
    ).create_ephemeral_client() as client:
        args = ['--grpc_host', client.host]
        if client.port:
            args.append('--grpc_port')
            args.append(client.port)
        if client.socket:
            args.append('--grpc_socket')
            args.append(client.socket)

        yield args


# This iterates over a list of contextmanagers that can be used to contruct
# (cli_args, uses_legacy_repository_yaml_format, instance tuples)
def valid_pipeline_args():
    for pipeline_target_args in valid_pipeline_target_args():
        yield args_with_default_instance(*pipeline_target_args)
        yield args_with_managed_grpc_instance(*pipeline_target_args)
    yield grpc_server_bar_kwargs(pipeline_name='foo')


def valid_execute_args():
    for pipeline_target_args in valid_pipeline_target_args():
        yield args_with_default_instance(*pipeline_target_args)


# [(cli_args, uses_legacy_repository_yaml_format)]
def valid_pipeline_target_args():
    return [
        (
            {
                'workspace': (file_relative_path(__file__, 'repository_file.yaml'),),
                'pipeline': 'foo',
                'python_file': None,
                'module_name': None,
                'attribute': None,
            },
            True,
        ),
        (
            {
                'workspace': (file_relative_path(__file__, 'repository_module.yaml'),),
                'pipeline': 'foo',
                'python_file': None,
                'module_name': None,
                'attribute': None,
            },
            True,
        ),
        (
            {
                'workspace': None,
                'pipeline': 'foo',
                'python_file': file_relative_path(__file__, 'test_cli_commands.py'),
                'module_name': None,
                'attribute': 'bar',
            },
            False,
        ),
        (
            {
                'workspace': None,
                'pipeline': 'foo',
                'python_file': file_relative_path(__file__, 'test_cli_commands.py'),
                'module_name': None,
                'attribute': 'bar',
                'working_directory': os.path.dirname(__file__),
            },
            False,
        ),
        (
            {
                'workspace': None,
                'pipeline': 'foo',
                'python_file': None,
                'module_name': 'dagster_tests.cli_tests.command_tests.test_cli_commands',
                'attribute': 'bar',
            },
            False,
        ),
        (
            {
                'workspace': None,
                'pipeline': None,
                'python_file': None,
                'module_name': 'dagster_tests.cli_tests.command_tests.test_cli_commands',
                'attribute': 'foo_pipeline',
            },
            False,
        ),
        (
            {
                'workspace': None,
                'pipeline': None,
                'python_file': file_relative_path(__file__, 'test_cli_commands.py'),
                'module_name': None,
                'attribute': 'define_foo_pipeline',
            },
            False,
        ),
        (
            {
                'workspace': None,
                'pipeline': None,
                'python_file': file_relative_path(__file__, 'test_cli_commands.py'),
                'module_name': None,
                'attribute': 'define_foo_pipeline',
                'working_directory': os.path.dirname(__file__),
            },
            False,
        ),
        (
            {
                'workspace': None,
                'pipeline': None,
                'python_file': file_relative_path(__file__, 'test_cli_commands.py'),
                'module_name': None,
                'attribute': 'foo_pipeline',
            },
            False,
        ),
    ]


# [(cli_args, uses_legacy_repository_yaml_format)]
def valid_pipeline_target_cli_args():
    return [
        (['-w', file_relative_path(__file__, 'repository_file.yaml'), '-p', 'foo'], True),
        (['-w', file_relative_path(__file__, 'repository_module.yaml'), '-p', 'foo'], True),
        (['-w', file_relative_path(__file__, 'workspace.yaml'), '-p', 'foo'], False),
        (
            [
                '-w',
                file_relative_path(__file__, 'override.yaml'),
                '-w',
                file_relative_path(__file__, 'workspace.yaml'),
                '-p',
                'foo',
            ],
            False,
        ),
        (
            ['-f', file_relative_path(__file__, 'test_cli_commands.py'), '-a', 'bar', '-p', 'foo'],
            False,
        ),
        (
            [
                '-f',
                file_relative_path(__file__, 'test_cli_commands.py'),
                '-d',
                os.path.dirname(__file__),
                '-a',
                'bar',
                '-p',
                'foo',
            ],
            False,
        ),
        (
            [
                '-m',
                'dagster_tests.cli_tests.command_tests.test_cli_commands',
                '-a',
                'bar',
                '-p',
                'foo',
            ],
            False,
        ),
        (
            ['-m', 'dagster_tests.cli_tests.command_tests.test_cli_commands', '-a', 'foo_pipeline'],
            False,
        ),
        (
            [
                '-f',
                file_relative_path(__file__, 'test_cli_commands.py'),
                '-a',
                'define_foo_pipeline',
            ],
            False,
        ),
        (
            [
                '-f',
                file_relative_path(__file__, 'test_cli_commands.py'),
                '-d',
                os.path.dirname(__file__),
                '-a',
                'define_foo_pipeline',
            ],
            False,
        ),
    ]


@pytest.mark.parametrize('gen_execute_args', valid_execute_args())
def test_scaffold_command(gen_execute_args):
    with gen_execute_args as (cli_args, uses_legacy_repository_yaml_format, _instance):
        if uses_legacy_repository_yaml_format:
            with pytest.warns(
                UserWarning,
                match=re.escape(
                    'You are using the legacy repository yaml format. Please update your file '
                ),
            ):
                cli_args['print_only_required'] = True
                execute_scaffold_command(cli_args=cli_args, print_fn=no_print)

                cli_args['print_only_required'] = False
                execute_scaffold_command(cli_args=cli_args, print_fn=no_print)
        else:
            cli_args['print_only_required'] = True
            execute_scaffold_command(cli_args=cli_args, print_fn=no_print)

            cli_args['print_only_required'] = False
            execute_scaffold_command(cli_args=cli_args, print_fn=no_print)


@pytest.mark.parametrize('execute_cli_args', valid_pipeline_target_cli_args())
def test_scaffold_command_cli(execute_cli_args):
    cli_args, uses_legacy_repository_yaml_format = execute_cli_args

    runner = CliRunner()

    if uses_legacy_repository_yaml_format:
        with pytest.warns(
            UserWarning,
            match=re.escape(
                'You are using the legacy repository yaml format. Please update your file '
            ),
        ):
            result = runner.invoke(pipeline_scaffold_command, cli_args)
            assert result.exit_code == 0

            result = runner.invoke(pipeline_scaffold_command, ['--print-only-required'] + cli_args)
            assert result.exit_code == 0
    else:
        result = runner.invoke(pipeline_scaffold_command, cli_args)
        assert result.exit_code == 0

        result = runner.invoke(pipeline_scaffold_command, ['--print-only-required'] + cli_args)
        assert result.exit_code == 0


def test_run_list():
    runner = CliRunner()
    result = runner.invoke(run_list_command)
    assert result.exit_code == 0


def test_run_wipe_correct_delete_message():
    runner = CliRunner()
    result = runner.invoke(run_wipe_command, input="DELETE\n")
    assert 'Deleted all run history and event logs' in result.output
    assert result.exit_code == 0


def test_run_wipe_incorrect_delete_message():
    runner = CliRunner()
    result = runner.invoke(run_wipe_command, input="WRONG\n")
    assert 'Exiting without deleting all run history and event logs' in result.output
    assert result.exit_code == 0


@contextmanager
def scheduler_instance(overrides=None):
    with seven.TemporaryDirectory() as temp_dir:
        overrides = merge_dicts(
            {
                'scheduler': {
                    'module': 'dagster.utils.test',
                    'class': 'FilesystemTestScheduler',
                    'config': {'base_dir': temp_dir},
                }
            },
            overrides if overrides else {},
        )
        with environ({'DAGSTER_HOME': temp_dir}):
            with open(os.path.join(temp_dir, 'dagster.yaml'), 'w') as fd:
                yaml.dump(overrides, fd, default_flow_style=False)
            yield DagsterInstance.get()


@contextmanager
def managed_grpc_scheduler_instance():
    with scheduler_instance(overrides={"opt_in": {"local_servers": True}}) as instance:
        yield instance


@contextmanager
def grpc_server_scheduler_cli_args():
    with grpc_server_bar_cli_args() as args:
        with scheduler_instance() as instance:
            yield args, instance


# This iterates over a list of contextmanagers that can be used to contruct
# (cli_args, instance tuples)
def valid_schedule_args():
    yield args_with_instance(
        scheduler_instance(), ['-w', file_relative_path(__file__, 'workspace.yaml')]
    )
    yield args_with_instance(
        managed_grpc_scheduler_instance(), ['-w', file_relative_path(__file__, 'workspace.yaml')]
    )
    yield grpc_server_scheduler_cli_args()


# This iterates over a list of contextmanagers that can be used to contruct
# (cli_args, use_legacy_repo_format, instance tuples)
def valid_backfill_args():
    yield args_with_instance(
        mocked_instance(),
        ['-w', file_relative_path(__file__, 'repository_file.yaml'), '--noprompt'],
    )
    yield args_with_instance(
        managed_grpc_instance(),
        ['-w', file_relative_path(__file__, 'repository_file.yaml'), '--noprompt'],
    )


def backfill_cli_runner_args(execution_args):
    return merge_dicts(
        execution_args,
        {'noprompt': True, 'workspace': (file_relative_path(__file__, 'repository_file.yaml'),),},
    )


def run_test_backfill(execution_args, expected_count=None, error_message=None):
    with mocked_instance() as instance:
        if error_message:
            with pytest.raises(Exception):
                execute_backfill_command(backfill_cli_runner_args(execution_args), print, instance)
        elif expected_count:
            execute_backfill_command(backfill_cli_runner_args(execution_args), print, instance)
            assert instance.get_runs_count() == expected_count


def test_backfill_no_pipeline():
    args = ['--pipeline', 'nonexistent']
    with pytest.warns(
        UserWarning,
        match=re.escape(
            'You are using the legacy repository yaml format. Please update your file '
        ),
    ):
        run_test_backfill(args, error_message='No pipeline found')


def test_backfill_no_partition_sets():
    args = ['--pipeline', 'foo']
    with pytest.warns(
        UserWarning,
        match=re.escape(
            'You are using the legacy repository yaml format. Please update your file '
        ),
    ):
        run_test_backfill(args, error_message='No partition sets found')


def test_backfill_no_named_partition_set():
    args = ['--pipeline', 'baz', '--partition-set', 'nonexistent']
    with pytest.warns(
        UserWarning,
        match=re.escape(
            'You are using the legacy repository yaml format. Please update your file '
        ),
    ):
        run_test_backfill(args, error_message='No partition set found')


def test_backfill_launch():
    args = {'pipeline': 'baz', 'partition-set': 'baz_partitions'}
    with pytest.warns(
        UserWarning,
        match=re.escape(
            'You are using the legacy repository yaml format. Please update your file '
        ),
    ):
        run_test_backfill(args, expected_count=len(string.digits))


def test_backfill_partition_range():
    args = ['--pipeline', 'baz', '--partition-set', 'baz_partitions', '--from', 'x']
    with pytest.warns(
        UserWarning,
        match=re.escape(
            'You are using the legacy repository yaml format. Please update your file '
        ),
    ):
        run_test_backfill(args, expected_count=3)

    args = ['--pipeline', 'baz', '--partition-set', 'baz_partitions', '--to', 'c']
    with pytest.warns(
        UserWarning,
        match=re.escape(
            'You are using the legacy repository yaml format. Please update your file '
        ),
    ):
        run_test_backfill(args, expected_count=3)

    args = ['--pipeline', 'baz', '--partition-set', 'baz_partitions', '--from', 'c', '--to', 'f']
    with pytest.warns(
        UserWarning,
        match=re.escape(
            'You are using the legacy repository yaml format. Please update your file '
        ),
    ):
        run_test_backfill(args, expected_count=4)


def test_backfill_partition_enum():
    args = ['--pipeline', 'baz', '--partition-set', 'baz_partitions', '--partitions', 'c,x,z']
    with pytest.warns(
        UserWarning,
        match=re.escape(
            'You are using the legacy repository yaml format. Please update your file '
        ),
    ):
        run_test_backfill(args, expected_count=3)


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


def test_backfill_tags_pipeline():
    runner = CliRunner()
    with mocked_instance() as instance:
        with pytest.warns(
            UserWarning,
            match=re.escape(
                'You are using the legacy repository yaml format. Please update your file '
            ),
        ):
            result = runner.invoke(
                pipeline_backfill_command,
                [
                    '-w',
                    file_relative_path(__file__, 'repository_file.yaml'),
                    '--noprompt',
                    '--partition-set',
                    'baz_partitions',
                    '--partitions',
                    'c',
                    '--tags',
                    '{ "foo": "bar" }',
                    '-p',
                    'baz',
                ],
            )
        assert result.exit_code == 0, result.stdout
        runs = instance.get_runs()
        assert len(runs) == 1
        run = runs[0]
        assert len(run.tags) >= 1
        assert run.tags.get('foo') == 'bar'


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
