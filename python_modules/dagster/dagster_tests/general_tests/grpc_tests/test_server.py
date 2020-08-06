import os
import sys
import threading
import time

import grpc
import pytest

from dagster import (
    DagsterInstance,
    Field,
    Int,
    Materialization,
    Output,
    pipeline,
    repository,
    solid,
)
from dagster.core.code_pointer import CodePointer
from dagster.core.origin import (
    PipelineGrpcServerOrigin,
    RepositoryGrpcServerOrigin,
    RepositoryPythonOrigin,
)
from dagster.grpc.client import ephemeral_grpc_api_client
from dagster.grpc.types import ExecuteRunArgs, LoadableTargetOrigin


@solid(config_schema={'n_events': Field(Int)})
def streaming_solid(context):
    for i in range(context.solid_config['n_events']):
        yield Materialization(label=str(i))
    yield Output('done')


@pipeline
def streaming_pipeline():
    streaming_solid()


@repository
def streaming_repository():
    return [streaming_pipeline]


STREAMING_PIPELINE_LOADABLE_TARGET_ORIGIN = LoadableTargetOrigin.from_python_origin(
    RepositoryPythonOrigin(
        executable_path=sys.executable,
        code_pointer=CodePointer.from_python_file(
            python_file=__file__, definition='streaming_repository', working_directory=os.getcwd()
        ),
    )
)


def _execute_run(api_client, instance, pipeline_run):
    return api_client.execute_run(
        execute_run_args=ExecuteRunArgs(
            pipeline_origin=PipelineGrpcServerOrigin(
                pipeline_name='streaming_pipeline',
                repository_origin=RepositoryGrpcServerOrigin(
                    host='localhost',
                    port=api_client.port,
                    socket=api_client.socket,
                    repository_name='streaming_repository',
                ),
            ),
            pipeline_run_id=pipeline_run.run_id,
            instance_ref=instance.get_ref(),
        )
    )


def _streaming_execution_target(events, api_client, instance, pipeline_run):
    for event in _execute_run(api_client, instance, pipeline_run):
        events.append(event)


def _streaming_ping_target(results, api_client):
    for result in api_client.streaming_ping(sequence_length=10000, echo='foo'):
        results.append(result)


def test_streaming_terminate_hard_with_no_running_execution():
    with ephemeral_grpc_api_client(max_workers=2) as api_client:
        streaming_results = []
        stream_events_result_thread = threading.Thread(
            target=_streaming_ping_target, args=[streaming_results, api_client]
        )
        stream_events_result_thread.daemon = True
        stream_events_result_thread.start()
        while not streaming_results:
            time.sleep(0.001)
        res = api_client.shutdown_server(hard=True)
        assert res.success
        assert res.serializable_error_info is None

        stream_events_result_thread.join()
        assert len(streaming_results) < 10000

        api_client._server_process.wait()  # pylint: disable=protected-access
        assert api_client._server_process.poll() == 0  # pylint: disable=protected-access


def test_streaming_terminate_soft_with_no_running_execution():
    with ephemeral_grpc_api_client(max_workers=2) as api_client:
        streaming_results = []
        stream_events_result_thread = threading.Thread(
            target=_streaming_ping_target, args=[streaming_results, api_client]
        )
        stream_events_result_thread.daemon = True
        stream_events_result_thread.start()
        while not streaming_results:
            time.sleep(0.1)
        res = api_client.shutdown_server(hard=False)
        assert res.success
        assert res.serializable_error_info is None

        stream_events_result_thread.join()
        assert len(streaming_results) < 10000

        api_client._server_process.wait()  # pylint: disable=protected-access
        assert api_client._server_process.poll() == 0  # pylint: disable=protected-access


def test_streaming_terminate_hard_with_running_execution():
    with ephemeral_grpc_api_client(
        loadable_target_origin=STREAMING_PIPELINE_LOADABLE_TARGET_ORIGIN, max_workers=2
    ) as api_client:
        instance = DagsterInstance.local_temp()
        pipeline_run = instance.create_run_for_pipeline(
            pipeline_def=streaming_pipeline,
            run_config={'solids': {'streaming_solid': {'config': {'n_events': 20}}}},
        )
        events = []
        execution_thread = threading.Thread(
            target=_streaming_execution_target, args=(events, api_client, instance, pipeline_run)
        )
        execution_thread.daemon = True
        execution_thread.start()
        while not events:
            time.sleep(0.001)
        res = api_client.shutdown_server(hard=True)
        assert res.success
        assert res.serializable_error_info is None

        execution_thread.join()
        assert (
            len([event for event in events if event.event_type_value == 'STEP_MATERIALIZATION'])
            < 20
        )

        api_client._server_process.wait()  # pylint: disable=protected-access
        assert api_client._server_process.poll() == 0  # pylint: disable=protected-access


def test_streaming_terminate_soft_with_running_execution():
    with ephemeral_grpc_api_client(
        loadable_target_origin=STREAMING_PIPELINE_LOADABLE_TARGET_ORIGIN, max_workers=2
    ) as api_client:
        instance = DagsterInstance.local_temp()
        pipeline_run = instance.create_run_for_pipeline(
            pipeline_def=streaming_pipeline,
            run_config={'solids': {'streaming_solid': {'config': {'n_events': 20}}}},
        )
        events = []
        execution_thread = threading.Thread(
            target=_streaming_execution_target, args=(events, api_client, instance, pipeline_run)
        )
        execution_thread.daemon = True
        execution_thread.start()
        while not events:
            time.sleep(0.001)
        res = api_client.shutdown_server(hard=False)
        assert res.success
        assert res.serializable_error_info is None

        execution_thread.join()
        assert (
            len([event for event in events if event.event_type_value == 'STEP_MATERIALIZATION'])
            == 20
        )

        api_client._server_process.wait()  # pylint: disable=protected-access
        assert api_client._server_process.poll() == 0  # pylint: disable=protected-access


def test_streaming_terminate_soft_idempotence_with_running_execution():
    with ephemeral_grpc_api_client(
        loadable_target_origin=STREAMING_PIPELINE_LOADABLE_TARGET_ORIGIN, max_workers=2
    ) as api_client:
        instance = DagsterInstance.local_temp()
        pipeline_run = instance.create_run_for_pipeline(
            pipeline_def=streaming_pipeline,
            run_config={'solids': {'streaming_solid': {'config': {'n_events': 20}}}},
        )
        events = []
        execution_thread = threading.Thread(
            target=_streaming_execution_target, args=(events, api_client, instance, pipeline_run)
        )
        execution_thread.daemon = True
        execution_thread.start()
        while not events:
            time.sleep(0.001)
        res = api_client.shutdown_server(hard=False)
        assert res.success
        assert res.serializable_error_info is None
        res = api_client.shutdown_server(hard=False)
        assert res.success
        assert res.serializable_error_info is None

        execution_thread.join()
        assert (
            len([event for event in events if event.event_type_value == 'STEP_MATERIALIZATION'])
            == 20
        )

        api_client._server_process.wait()  # pylint: disable=protected-access
        assert api_client._server_process.poll() == 0  # pylint: disable=protected-access


def test_streaming_terminate_hard_idempotence_with_running_execution():
    with ephemeral_grpc_api_client(
        loadable_target_origin=STREAMING_PIPELINE_LOADABLE_TARGET_ORIGIN, max_workers=2
    ) as api_client:
        instance = DagsterInstance.local_temp()
        pipeline_run = instance.create_run_for_pipeline(
            pipeline_def=streaming_pipeline,
            run_config={'solids': {'streaming_solid': {'config': {'n_events': 20}}}},
        )
        events = []
        execution_thread = threading.Thread(
            target=_streaming_execution_target, args=(events, api_client, instance, pipeline_run)
        )
        execution_thread.daemon = True
        execution_thread.start()
        while not events:
            time.sleep(0.001)
        res = api_client.shutdown_server(hard=False)
        assert res.success
        assert res.serializable_error_info is None
        res = api_client.shutdown_server(hard=True)
        assert res.success
        assert res.serializable_error_info is None

        execution_thread.join()
        assert (
            len([event for event in events if event.event_type_value == 'STEP_MATERIALIZATION'])
            < 20
        )

        api_client._server_process.wait()  # pylint: disable=protected-access
        assert api_client._server_process.poll() == 0  # pylint: disable=protected-access


def test_streaming_terminate_soft_gate():
    with ephemeral_grpc_api_client(
        loadable_target_origin=STREAMING_PIPELINE_LOADABLE_TARGET_ORIGIN, max_workers=2
    ) as api_client:
        instance = DagsterInstance.local_temp()
        pipeline_run = instance.create_run_for_pipeline(
            pipeline_def=streaming_pipeline,
            run_config={'solids': {'streaming_solid': {'config': {'n_events': 20}}}},
        )
        events = []
        execution_thread = threading.Thread(
            target=_streaming_execution_target, args=(events, api_client, instance, pipeline_run)
        )
        execution_thread.daemon = True
        execution_thread.start()
        while not events:
            time.sleep(0.001)
        res = api_client.shutdown_server(hard=False)
        assert res.success
        assert res.serializable_error_info is None
        with pytest.raises(grpc._channel._InactiveRpcError):  # pylint: disable=protected-access
            api_client.ping(echo='foo')

        gated_pipeline_run = instance.create_run_for_pipeline(
            pipeline_def=streaming_pipeline,
            run_config={'solids': {'streaming_solid': {'config': {'n_events': 20}}}},
        )
        with pytest.raises(
            grpc._channel._MultiThreadedRendezvous  # pylint: disable=protected-access
        ):
            _ = [event for event in _execute_run(api_client, instance, gated_pipeline_run)]

        execution_thread.join()
        assert (
            len([event for event in events if event.event_type_value == 'STEP_MATERIALIZATION'])
            == 20
        )

        api_client._server_process.wait()  # pylint: disable=protected-access
        assert api_client._server_process.poll() == 0  # pylint: disable=protected-access

