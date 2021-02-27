import time
from unittest import mock

import pytest
from dagster.daemon.daemon import SensorDaemon
from dagster.daemon.types import DaemonHeartbeat
from dagster_graphql.test.utils import execute_dagster_graphql

from .graphql_context_test_suite import (
    GraphQLContextVariant,
    ReadonlyGraphQLContextTestMatrix,
    make_graphql_context_test_suite,
)

RUN_QUEUING_QUERY = """
query InstanceDetailSummaryQuery {
    instance {
        runQueuingSupported
    }
}
"""

INSTANCE_HEALTH_QUERY = """
query InstanceHealthQuery {
    instance {
        instanceHealth {
            healthy
            unhealthyDaemons {
                daemonType
            }
        }
    }
}
"""


class TestQueued(
    make_graphql_context_test_suite(
        context_variants=[
            GraphQLContextVariant.sqlite_with_queued_run_coordinator_managed_grpc_env(),
        ]
    )
):
    def test_get_individual_daemons(self, graphql_context):
        results = execute_dagster_graphql(graphql_context, RUN_QUEUING_QUERY)
        assert results.data == {"instance": {"runQueuingSupported": True}}


class TestInstance(
    make_graphql_context_test_suite(
        context_variants=[GraphQLContextVariant.readonly_sqlite_instance_deployed_grpc_env()]
    )
):
    def test_instance_healthy(self, graphql_context):
        if graphql_context.instance.is_ephemeral:
            pytest.skip("The daemon isn't compatible with an in-memory instance")
        results = execute_dagster_graphql(graphql_context, INSTANCE_HEALTH_QUERY)
        assert results.data
        assert results.data["instance"]["instanceHealth"]["healthy"] == False
        assert results.data["instance"]["instanceHealth"]["unhealthyDaemons"] == [
            {"daemonType": "SENSOR"}
        ]

        graphql_context.instance.add_daemon_heartbeat(
            DaemonHeartbeat(
                timestamp=time.time(),
                daemon_type=SensorDaemon.daemon_type(),
                daemon_id=None,
                errors=None,
            )
        )

        results = execute_dagster_graphql(graphql_context, INSTANCE_HEALTH_QUERY)
        assert results.data["instance"]["instanceHealth"]["healthy"] == True
        assert results.data["instance"]["instanceHealth"]["unhealthyDaemons"] == []
