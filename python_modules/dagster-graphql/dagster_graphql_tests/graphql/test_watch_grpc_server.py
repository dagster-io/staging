import time

from dagster.core.host_representation.grpc_server_state_subscriber import (
    GrpcServerStateChangeEvent,
    GrpcServerStateChangeEventType,
    GrpcServerStateSubscriber,
)

from .graphql_context_test_suite import GraphQLContextVariant, make_graphql_context_test_suite


class TextSubscribeToGrpcServerEvents(
    make_graphql_context_test_suite(
        context_variants=[GraphQLContextVariant.readonly_sqlite_instance_deployed_grpc_env()]
    )
):
    def test_grpc_server_handle_message_subscription(self, graphql_context):
        events = []
        test_subscriber = GrpcServerStateSubscriber(events.append)
        handle = next(
            graphql_context._workspace.repository_location_handles  # pylint: disable=protected-access
        )
        handle.add_grpc_server_state_subscriber(test_subscriber)
        handle.client.shutdown_server()
        time.sleep(1)

        assert len(events) == 1
        assert isinstance(events[0], GrpcServerStateChangeEvent)
        assert events[0].event_type == GrpcServerStateChangeEventType.SERVER_DISCONNECTED
