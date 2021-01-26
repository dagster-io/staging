import logging
import time
from collections import defaultdict

from dagster import DagsterEvent, DagsterEventType, check
from dagster.core.events.log import DagsterEventRecord
from dagster.core.host_representation.handle import RepositoryLocationHandle
from dagster.core.storage.pipeline_run import (
    IN_PROGRESS_RUN_STATUSES,
    PipelineRun,
    PipelineRunStatus,
    PipelineRunsFilter,
)
from dagster.core.storage.tags import PRIORITY_TAG
from dagster.daemon.daemon import DagsterDaemon
from dagster.daemon.types import DaemonType
from dagster.utils.backcompat import experimental
from dagster.utils.external import external_pipeline_from_location_handle


class DagsterCloudMetadataDaemon(DagsterDaemon):
    """
    On each loop, answers incoming requests initiated from teh host cloud.
    """

    def __init__(
        self, instance,
    ):
        super(DagsterCloudMetadataDaemon, self).__init__(instance)

        # Fetch which repository locations have been registered and
        # where to find them, from a storage scheme in dagster cloud
        # (The same storage used to initialize the Dagster Cloud workspace)

        grpc_host_infos = dagster_cloud.api.get_hosts()

        self.grpc_clients = {
            grpc_host_info.location_name : DagsterGrpcClient(
                port=grpc_host_info.port, host=grpc_host_info.host
            )
            for grpc_host_info in grpc_host_infos
        }
        self.queue_client = new DagsterCloudMessageQueueClient()

    @classmethod
    def daemon_type(cls):
        return DaemonType.DAGSTER_CLOUD_METADATA

    # This would need to run at much lower latency than the other daemons
    # and not get blocked on them, which would require some changes the
    # the dagster-daemon setup
    def run_iteration(self):
        while (self.queue_client.has_request())
            queue_request = self.queue_client.pop_request()

            location_name = queue_request.location_name
            request_id = queue_request.request_id
            request_api_name = queue_request.request_api_name
            request_args = queue_request.request_args

            response = self.grpc_clients[location_name].make_api_request(
                request_api_name,
                request_args
            )

            self.queue_client.add_response(request_id, serialized_response)
