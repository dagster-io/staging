from dagster import check

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

        if origin_id not in self._location_handles:
            if isinstance(repository_location_origin, ManagedGrpcPythonEnvRepositoryLocationOrigin):
                handle = self._grpc_server_registry.get_grpc_handle(repository_location_origin)
            else:
                handle = repository_location_origin.create_handle()

            self._location_handles[origin_id] = handle

        return self._location_handles[origin_id]

    def cleanup(self):
        for handle in self._location_handles.values():
            handle.cleanup()

    def __exit__(self, exception_type, exception_value, traceback):
        self.cleanup()
