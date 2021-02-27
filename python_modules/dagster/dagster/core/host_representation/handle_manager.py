from dagster import check

from .handle import GrpcServerRepositoryLocationHandle
from .origin import ManagedGrpcPythonEnvRepositoryLocationOrigin, RepositoryLocationOrigin


class RepositoryLocationHandleManager:
    """
    Holds repository location handles for reuse
    """

    def __init__(self, grpc_server_registry):
        from dagster.core.host_representation.grpc_server_registry import GrpcServerRegistry

        self._location_handles = {}

        self._grpc_server_registry = check.inst_param(
            grpc_server_registry, "grpc_server_registry", GrpcServerRegistry
        )

    def __enter__(self):
        return self

    def get_handle(self, repository_location_origin):
        check.inst_param(
            repository_location_origin, "repository_location_origin", RepositoryLocationOrigin
        )
        origin_id = repository_location_origin.get_id()
        existing_handle = self._location_handles.get(origin_id)

        if isinstance(repository_location_origin, ManagedGrpcPythonEnvRepositoryLocationOrigin):
            status = self._grpc_server_registry.get_grpc_status(repository_location_origin)
            if existing_handle and existing_handle.server_id != status.server_id:
                existing_handle.cleanup()
                existing_handle = None

            handle = (
                existing_handle
                if existing_handle
                else GrpcServerRepositoryLocationHandle(
                    origin=repository_location_origin,
                    server_id=status.server_id,
                    port=status.port,
                    socket=status.socket,
                    host="localhost",
                    heartbeat=True,
                    watch_server=False,
                )
            )

        else:
            handle = repository_location_origin.create_handle()

        self._location_handles[origin_id] = handle
        return self._location_handles[origin_id]

    def cleanup(self):
        for handle in self._location_handles.values():
            handle.cleanup()

    def __exit__(self, exception_type, exception_value, traceback):
        self.cleanup()
