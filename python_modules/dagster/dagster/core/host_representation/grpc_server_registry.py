import sys
import threading
from contextlib import AbstractContextManager

from dagster.core.errors import DagsterUserCodeProcessError
from dagster.core.host_representation.handle import SharedGrpcRepositoryLocationHandle
from dagster.core.host_representation.origin import (
    GrpcServerRepositoryLocationOrigin,
    ManagedGrpcPythonEnvRepositoryLocationOrigin,
)
from dagster.grpc.server import GrpcServerProcess
from dagster.utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info


# Daemons in different threads can use a shared GrpcServerRegistry to ensure that
# a single GrpcServerProcess is created for each origin.
class GrpcServerRegistry(AbstractContextManager):
    def __init__(self):
        self._grpc_processes = {}
        self._lock = threading.Lock()

    # Always returns a new handle - threads should do their own memoization to avoid
    # creating a bunch of handles for the same origin (e.g. using a RepositoryLocationHandleManager)
    def get_grpc_handle(self, repository_location_origin):
        if isinstance(repository_location_origin, GrpcServerRepositoryLocationOrigin):
            return repository_location_origin.create_handle()
        elif isinstance(repository_location_origin, ManagedGrpcPythonEnvRepositoryLocationOrigin):
            with self._lock:
                origin_id = repository_location_origin.get_id()
                if not origin_id in self._grpc_processes:
                    try:
                        server_process = GrpcServerProcess(
                            loadable_target_origin=repository_location_origin.loadable_target_origin,
                            heartbeat=True,
                        )
                    except Exception:  # pylint: disable=broad-except
                        server_process = serializable_error_info_from_exc_info(sys.exc_info())

                    self._grpc_processes[origin_id] = server_process

                process = self._grpc_processes[origin_id]

                if isinstance(process, SerializableErrorInfo):
                    raise DagsterUserCodeProcessError(
                        process.to_string(), user_code_process_error_infos=[process]
                    )

                return SharedGrpcRepositoryLocationHandle(
                    repository_location_origin, port=process.port, socket=process.socket
                )

    def check_for_reloads(self):
        # MAJOR MISSING PIECE HERE - NEED TO ACQUIRE THE LOCK AND RELOAD ANY MANAGED CODE
        # EVERY 30 SECONDS
        pass

    def __exit__(self, exception_type, exception_value, traceback):
        for process in self._grpc_processes.values():
            process.create_ephemeral_client().cleanup_server()

    def wait_for_processes(self):
        for process in self._grpc_processes.values():
            process.wait()
