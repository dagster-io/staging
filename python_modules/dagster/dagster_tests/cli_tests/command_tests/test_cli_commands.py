from __future__ import print_function

import os
import string
from contextlib import contextmanager

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
from dagster.cli.run import run_list_command, run_wipe_command
from dagster.core.test_utils import mocked_instance, mocked_instance_tempdir
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


def not_a_repo_or_pipeline_fn():
    return 'kdjfkjdf'


not_a_repo_or_pipeline = 123


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


@contextmanager
def scheduler_instance(overrides=None):
    with seven.TemporaryDirectory() as temp_dir:
        with mocked_instance_tempdir(
            temp_dir,
            overrides=merge_dicts(
                {
                    'scheduler': {
                        'module': 'dagster.utils.test',
                        'class': 'FilesystemTestScheduler',
                        'config': {'base_dir': temp_dir},
                    }
                },
                overrides if overrides else {},
            ),
        ) as instance:
            yield instance


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
    repo_args = {
        'noprompt': True,
        'python_file': file_relative_path(__file__, 'test_cli_commands.py'),
        'attribute': 'bar',
    }
    yield args_with_instance(mocked_instance(), repo_args)
    yield args_with_instance(managed_grpc_instance(), repo_args)
    yield grpc_server_backfill_args()


@contextmanager
def grpc_server_backfill_args():
    with grpc_server_bar_kwargs() as (args, _use_legacy_repo_format, instance):
        yield merge_dicts(args, {'noprompt': True}), instance


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
