import time

from dagster import check

from .handle import GrpcServerRepositoryLocationHandle
from .origin import RepositoryLocationOrigin


class RepositoryLocationHandleManager:
    """
    Holds repository location handles for reuse
    """

    def __init__(self, grpc_server_registry):
        from dagster.core.host_representation.grpc_server_registry import GrpcServerRegistry

        self._location_handles = {}  # Origin ID to (handle, creation timestamp) tuples

        self._grpc_server_registry = check.inst_param(
            grpc_server_registry, "grpc_server_registry", GrpcServerRegistry
        )

    def __enter__(self):
        return self

    def get_handle(self, repository_location_origin, min_time=None):
        check.inst_param(
            repository_location_origin, "repository_location_origin", RepositoryLocationOrigin
        )
        origin_id = repository_location_origin.get_id()
        existing_handle_entry = self._location_handles.get(origin_id)

        handle = None
        creation_timestamp = None
        if existing_handle_entry:
            handle, creation_timestamp = existing_handle_entry

        if not self._grpc_server_registry.supports_origin(repository_location_origin):
            if handle:
                if min_time and creation_timestamp < min_time:
                    handle.cleanup()
                    handle = None
                    del self._location_handles[origin_id]

            if not handle:
                handle = repository_location_origin.create_handle()
                creation_timestamp = time.time()

        else:
            # Registry will return a new endpoint if the existing one is from
            # before the minimum time
            endpoint = self._grpc_server_registry.get_grpc_endpoint(
                repository_location_origin, min_time
            )

            # Registry may periodically reload the endpoint, at which point the server ID will
            # change and we should reload the handle
            if handle:
                if handle.server_id != endpoint.server_id:
                    handle.cleanup()
                    handle = None
                    del self._location_handles[origin_id]

            if not handle:
                handle = GrpcServerRepositoryLocationHandle(
                    origin=repository_location_origin,
                    server_id=endpoint.server_id,
                    port=endpoint.port,
                    socket=endpoint.socket,
                    host=endpoint.host,
                    heartbeat=True,
                    watch_server=False,
                )
                creation_timestamp = time.time()

        self._location_handles[origin_id] = (handle, creation_timestamp)
        return handle

    def cleanup(self):
        for handle, timestamp in self._location_handles.values():
            handle.cleanup()

    def __exit__(self, exception_type, exception_value, traceback):
        self.cleanup()
