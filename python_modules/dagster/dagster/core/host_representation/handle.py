import sys
import threading
import time
from abc import ABCMeta, abstractmethod
from collections import namedtuple

import grpc
import six
from dagster import check
from dagster.api.list_repositories import sync_list_repositories_grpc
from dagster.core.definitions.reconstructable import repository_def_from_pointer
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.host_representation.origin import (
    ExternalRepositoryOrigin,
    GrpcServerRepositoryLocationOrigin,
    InProcessRepositoryLocationOrigin,
    ManagedGrpcPythonEnvRepositoryLocationOrigin,
    RepositoryLocationOrigin,
)
from dagster.core.host_representation.selector import PipelineSelector
from dagster.core.host_representation.state import (
    HandleStateChangeEvent,
    HandleStateChangeEventType,
    HandleStateChangeSubscriber,
)
from dagster.core.instance import DagsterInstance
from dagster.core.origin import RepositoryGrpcServerOrigin, RepositoryOrigin, RepositoryPythonOrigin


def _get_repository_python_origin(executable_path, repository_code_pointer_dict, repository_name):
    if repository_name not in repository_code_pointer_dict:
        raise DagsterInvariantViolationError(
            "Unable to find repository name {} on GRPC server.".format(repository_name)
        )

    code_pointer = repository_code_pointer_dict[repository_name]
    return RepositoryPythonOrigin(executable_path=executable_path, code_pointer=code_pointer)


class RepositoryLocationHandle(six.with_metaclass(ABCMeta)):
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.cleanup()

    def cleanup(self):
        pass

    @staticmethod
    def create_from_repository_location_origin(repo_location_origin):
        check.inst_param(repo_location_origin, "repo_location_origin", RepositoryLocationOrigin)
        if isinstance(repo_location_origin, ManagedGrpcPythonEnvRepositoryLocationOrigin):
            return ManagedGrpcPythonEnvRepositoryLocationHandle(repo_location_origin)
        elif isinstance(repo_location_origin, GrpcServerRepositoryLocationOrigin):
            return GrpcServerRepositoryLocationHandle(repo_location_origin)
        elif isinstance(repo_location_origin, InProcessRepositoryLocationOrigin):
            return InProcessRepositoryLocationHandle(repo_location_origin)
        else:
            check.failed("Unexpected repository location origin")

    @staticmethod
    def create_from_repository_origin(repository_origin, instance):
        check.inst_param(repository_origin, "repository_origin", RepositoryOrigin)
        check.inst_param(instance, "instance", DagsterInstance)

        if isinstance(repository_origin, RepositoryGrpcServerOrigin):
            return RepositoryLocationHandle.create_from_repository_location_origin(
                GrpcServerRepositoryLocationOrigin(
                    port=repository_origin.port,
                    socket=repository_origin.socket,
                    host=repository_origin.host,
                )
            )
        elif isinstance(repository_origin, RepositoryPythonOrigin):
            loadable_target_origin = repository_origin.loadable_target_origin

            repo_location_origin = ManagedGrpcPythonEnvRepositoryLocationOrigin(
                loadable_target_origin
            )

            return RepositoryLocationHandle.create_from_repository_location_origin(
                repo_location_origin
            )
        else:
            raise DagsterInvariantViolationError("Unexpected repository origin type")

    @abstractmethod
    def get_repository_python_origin(self, repository_name):
        pass


class GrpcServerRepositoryLocationHandle(RepositoryLocationHandle):
    """
    Represents a gRPC server that Dagster is not responsible for managing.
    """

    def __init__(self, origin):
        from dagster.grpc.client import DagsterGrpcClient

        self.origin = check.inst_param(origin, "origin", GrpcServerRepositoryLocationOrigin)

        port = self.origin.port
        socket = self.origin.socket
        host = self.origin.host

        self.client = DagsterGrpcClient(port=port, socket=socket, host=host)

        list_repositories_response = sync_list_repositories_grpc(self.client)

        self.repository_names = set(
            symbol.repository_name for symbol in list_repositories_response.repository_symbols
        )

        self.executable_path = list_repositories_response.executable_path
        self.repository_code_pointer_dict = list_repositories_response.repository_code_pointer_dict

        self._state_change_subscribers = []
        self._current_server_id = None

        self._watch_thread_shutdown_event = threading.Event()
        self._watch_thread = threading.Thread(
            target=self._watch_server_thread, args=[self._watch_thread_shutdown_event]
        )
        self._watch_thread.daemon = True
        self._watch_thread.start()

    @property
    def port(self):
        return self.origin.port

    @property
    def socket(self):
        return self.origin.socket

    @property
    def host(self):
        return self.origin.host

    @property
    def location_name(self):
        return self.origin.location_name

    def get_current_image(self):
        job_image = self.client.get_current_image().current_image
        if not job_image:
            raise DagsterInvariantViolationError(
                "Unable to get current image that GRPC server is running. Please make sure that "
                "env var DAGSTER_CURRENT_IMAGE is set in the GRPC server and contains the most "
                "up-to-date user code image and tag. Exiting."
            )
        return job_image

    def add_state_change_subscriber(self, handle_state_change_subscriber):
        check.inst_param(
            handle_state_change_subscriber,
            "handle_state_change_subscriber",
            HandleStateChangeSubscriber,
        )
        self._state_change_subscribers.append(handle_state_change_subscriber)

    def _send_state_change_event(self, event):
        for handle_state_change_subscriber in self._state_change_subscribers:
            handle_state_change_subscriber.handle_event(event)

    def _watch_server_thread(self, shutdown_event):
        """
        This thread watches the state of the unmanaged gRPC server and communicates
        any changes to subscribers. Subcribers can be added using the `add_state_change_subscriber`
        method.

        The following loop polls the GetServerId endpoint to check if either:
        1. The server_id has changed
        2. The server is unreachable

        In the case of (1) The server ID has changed, we send a SEVER_UPDATED event to the
        subscribers and end the thread.

        In the case of (2) The server is unreachable, we attempt to automatically reconnect. If we
        are able to reconnect, there are two possibilities:

        a. The server ID has changed
            -> In this case, we send an SERVER_UPDATED event to the subscribers, and we end
            the thread.
        b. The server ID is the same
            -> In this case, we send an SERVER_RECONNECT event to the subscribers, and we go back to
            polling the server for changes.

        If we are unable to reconnect to the server within the specified max_reconnect_attempts, we
        send a SERVER_ERROR event to the subscribers.

        The expectation is that on SERVER_UPDATED and SERVER_ERROR events, the subscibers dispose of
        this handle, create a new handle, and subscribe to events on the new handle.
        """

        print("hello")

        poll_interval = 1
        reconnect_interval = 1
        max_reconnect_attempts = 10

        def debug(message):
            if True:
                print(message)

        def watch_for_changes():
            while True:
                debug("Watching for changes")
                if shutdown_event.is_set():
                    break

                new_server_id = self.client.get_server_id()
                if not self._current_server_id:
                    self._current_server_id = new_server_id
                elif self._current_server_id != new_server_id:
                    return self._send_state_change_event(
                        HandleStateChangeEvent(
                            HandleStateChangeEventType.SERVER_UPDATED,
                            message="Handle {} has been updated.".format(self.location_name),
                            location_name=self.location_name,
                        )
                    )

                time.sleep(poll_interval)

        def reconnect_loop():
            attempts = 0
            while attempts < max_reconnect_attempts:
                if shutdown_event.is_set():
                    break
                time.sleep(reconnect_interval)
                try:
                    new_server_id = self.client.get_server_id()

                    if self._current_server_id != new_server_id:
                        self._send_state_change_event(
                            HandleStateChangeEvent(
                                HandleStateChangeEventType.SERVER_UPDATED,
                                message="Handle {} has been updated.".format(self.location_name),
                                location_name=self.location_name,
                            )
                        )
                        return False

                    self._send_state_change_event(
                        HandleStateChangeEvent(
                            HandleStateChangeEventType.SERVER_RECONNECTED,
                            message="Handle {} has been reconnected.".format(self.location_name),
                            location_name=self.location_name,
                        )
                    )
                    return True

                except grpc._channel._InactiveRpcError:  # pylint: disable=protected-access
                    attempts += 1
                    pass

            self._send_state_change_event(
                HandleStateChangeEvent(
                    HandleStateChangeEventType.SERVER_ERROR,
                    message="Handle {} has been errored.".format(self.location_name),
                    location_name=self.location_name,
                )
            )
            return False

        while True:
            print("Hello")
            if shutdown_event.is_set():
                break

            try:
                # Poll for changes. This watch_for_changes loop exits when either the server ID
                # changes, or if the gRPC request was unsuccessful.
                #
                # In the latter case, we go to the exception handler and attempt to reconect
                watch_for_changes()
                break
            except grpc._channel._InactiveRpcError:  # pylint: disable=protected-access
                debug("Failed query for server id. Going to start watching")
                self._send_state_change_event(
                    HandleStateChangeEvent(
                        HandleStateChangeEventType.SERVER_RECONNECTING,
                        message="Handle {} has has been disconnected. Attempting to reconnect.".format(
                            self.location_name
                        ),
                        location_name=self.location_name,
                    )
                )
                successful_reconnect = reconnect_loop()
                if not successful_reconnect:
                    break

    def cleanup(self):
        self._watch_thread_shutdown_event.set()
        # self._watch_thread.join()

    def get_repository_python_origin(self, repository_name):
        return _get_repository_python_origin(
            self.executable_path, self.repository_code_pointer_dict, repository_name
        )

    def reload_repository_python_origin(self, repository_name):
        check.str_param(repository_name, "repository_name")

        list_repositories_response = sync_list_repositories_grpc(self.client)

        return _get_repository_python_origin(
            list_repositories_response.executable_path,
            list_repositories_response.repository_code_pointer_dict,
            repository_name,
        )


class ManagedGrpcPythonEnvRepositoryLocationHandle(RepositoryLocationHandle):
    """
    A Python environment for which Dagster is managing a gRPC server.
    """

    def __init__(self, origin):
        from dagster.grpc.client import client_heartbeat_thread
        from dagster.grpc.server import GrpcServerProcess

        self.origin = check.inst_param(
            origin, "origin", ManagedGrpcPythonEnvRepositoryLocationOrigin
        )
        loadable_target_origin = origin.loadable_target_origin

        self.grpc_server_process = GrpcServerProcess(
            loadable_target_origin=loadable_target_origin,
            max_workers=2,
            heartbeat=True,
            lazy_load_user_code=True,
        )
        self.client = self.grpc_server_process.create_ephemeral_client()

        self.heartbeat_shutdown_event = threading.Event()

        self.heartbeat_thread = threading.Thread(
            target=client_heartbeat_thread, args=(self.client, self.heartbeat_shutdown_event)
        )
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        list_repositories_response = sync_list_repositories_grpc(self.client)

        self.repository_code_pointer_dict = list_repositories_response.repository_code_pointer_dict

    def get_repository_python_origin(self, repository_name):
        return _get_repository_python_origin(
            self.executable_path, self.repository_code_pointer_dict, repository_name
        )

    @property
    def executable_path(self):
        return self.loadable_target_origin.executable_path

    @property
    def location_name(self):
        return self.origin.location_name

    @property
    def loadable_target_origin(self):
        return self.origin.loadable_target_origin

    @property
    def repository_names(self):
        return set(self.repository_code_pointer_dict.keys())

    @property
    def host(self):
        return "localhost"

    @property
    def port(self):
        return self.grpc_server_process.port

    @property
    def socket(self):
        return self.grpc_server_process.socket

    def cleanup(self):
        self.heartbeat_shutdown_event.set()
        self.heartbeat_thread.join()
        self.client.cleanup_server()


class InProcessRepositoryLocationHandle(RepositoryLocationHandle):
    def __init__(self, origin):
        self.origin = check.inst_param(origin, "origin", InProcessRepositoryLocationOrigin)

        pointer = self.origin.recon_repo.pointer
        repo_def = repository_def_from_pointer(pointer)
        self.repository_code_pointer_dict = {repo_def.name: pointer}

    @property
    def location_name(self):
        return self.origin.location_name

    def get_repository_python_origin(self, repository_name):
        return _get_repository_python_origin(
            sys.executable, self.repository_code_pointer_dict, repository_name
        )


class RepositoryHandle(
    namedtuple("_RepositoryHandle", "repository_name repository_location_handle")
):
    def __new__(cls, repository_name, repository_location_handle):
        return super(RepositoryHandle, cls).__new__(
            cls,
            check.str_param(repository_name, "repository_name"),
            check.inst_param(
                repository_location_handle, "repository_location_handle", RepositoryLocationHandle
            ),
        )

    def get_origin(self):
        if isinstance(self.repository_location_handle, InProcessRepositoryLocationHandle):
            return RepositoryPythonOrigin(
                code_pointer=self.repository_location_handle.repository_code_pointer_dict[
                    self.repository_name
                ],
                executable_path=sys.executable,
            )
        elif isinstance(
            self.repository_location_handle, ManagedGrpcPythonEnvRepositoryLocationHandle
        ):
            return RepositoryPythonOrigin(
                code_pointer=self.repository_location_handle.repository_code_pointer_dict[
                    self.repository_name
                ],
                executable_path=self.repository_location_handle.executable_path,
            )
        elif isinstance(self.repository_location_handle, GrpcServerRepositoryLocationHandle):
            return RepositoryGrpcServerOrigin(
                host=self.repository_location_handle.host,
                port=self.repository_location_handle.port,
                socket=self.repository_location_handle.socket,
                repository_name=self.repository_name,
            )
        else:
            check.failed(
                "Can not target represented RepositoryDefinition locally for repository from a {}.".format(
                    self.repository_location_handle.__class__.__name__
                )
            )

    def get_external_origin(self):
        return ExternalRepositoryOrigin(
            self.repository_location_handle.origin, self.repository_name,
        )

    def get_python_origin(self):
        return self.repository_location_handle.get_repository_python_origin(self.repository_name)


class PipelineHandle(namedtuple("_PipelineHandle", "pipeline_name repository_handle")):
    def __new__(cls, pipeline_name, repository_handle):
        return super(PipelineHandle, cls).__new__(
            cls,
            check.str_param(pipeline_name, "pipeline_name"),
            check.inst_param(repository_handle, "repository_handle", RepositoryHandle),
        )

    def to_string(self):
        return "{self.location_name}.{self.repository_name}.{self.pipeline_name}".format(self=self)

    @property
    def repository_name(self):
        return self.repository_handle.repository_name

    @property
    def location_name(self):
        return self.repository_handle.repository_location_handle.location_name

    def get_origin(self):
        return self.repository_handle.get_origin().get_pipeline_origin(self.pipeline_name)

    def get_external_origin(self):
        return self.repository_handle.get_external_origin().get_pipeline_origin(self.pipeline_name)

    def get_python_origin(self):
        return self.repository_handle.get_python_origin().get_pipeline_origin(self.pipeline_name)

    def to_selector(self):
        return PipelineSelector(self.location_name, self.repository_name, self.pipeline_name, None)


class ScheduleHandle(namedtuple("_ScheduleHandle", "schedule_name repository_handle")):
    def __new__(cls, schedule_name, repository_handle):
        return super(ScheduleHandle, cls).__new__(
            cls,
            check.str_param(schedule_name, "schedule_name"),
            check.inst_param(repository_handle, "repository_handle", RepositoryHandle),
        )

    @property
    def repository_name(self):
        return self.repository_handle.repository_name

    @property
    def location_name(self):
        return self.repository_handle.repository_location_handle.location_name

    def get_origin(self):
        return self.repository_handle.get_origin().get_schedule_origin(self.schedule_name)

    def get_external_origin(self):
        return self.repository_handle.get_external_origin().get_schedule_origin(self.schedule_name)


class PartitionSetHandle(namedtuple("_PartitionSetHandle", "partition_set_name repository_handle")):
    def __new__(cls, partition_set_name, repository_handle):
        return super(PartitionSetHandle, cls).__new__(
            cls,
            check.str_param(partition_set_name, "partition_set_name"),
            check.inst_param(repository_handle, "repository_handle", RepositoryHandle),
        )

    @property
    def repository_name(self):
        return self.repository_handle.repository_name

    @property
    def location_name(self):
        return self.repository_handle.repository_location_handle.location_name
