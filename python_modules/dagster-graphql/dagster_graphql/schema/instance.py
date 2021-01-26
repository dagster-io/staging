import sys

import graphene
from dagster import DagsterInstance, check
from dagster.daemon.types import DaemonStatus as PythonDaemonStatus
from dagster.daemon.types import DaemonType as PythonDaemonType

from .errors import PythonError
from .util import non_null_list


class RunLauncher(graphene.ObjectType):
    name = graphene.NonNull(graphene.String)

    def __init__(self, run_launcher):
        self._run_launcher = check.inst_param(run_launcher, "run_launcher", RunLauncher)

    def resolve_name(self, _graphene_info):
        return self._run_launcher.__class__.__name__


DaemonType = graphene.Enum.from_enum(PythonDaemonType)


class DaemonStatus(graphene.ObjectType):
    daemonType = graphene.NonNull(DaemonType)
    required = graphene.NonNull(graphene.Boolean)
    healthy = graphene.Boolean()
    lastHeartbeatTime = graphene.Float()
    lastHeartbeatError = graphene.Field(PythonError)

    def __init__(self, daemon_status):
        check.inst_param(daemon_status, "daemon_status", DaemonStatus)

        last_heartbeat_time = None
        last_heartbeat_error = None
        if daemon_status.last_heartbeat:
            last_heartbeat_time = daemon_status.last_heartbeat.timestamp
            if daemon_status.last_heartbeat.error:
                last_heartbeat_error = PythonError(daemon_status.last_heartbeat.error)

        super(DaemonStatus, self).__init__(
            daemonType=daemon_status.daemon_type,
            required=daemon_status.required,
            healthy=daemon_status.healthy,
            lastHeartbeatTime=last_heartbeat_time,
            lastHeartbeatError=last_heartbeat_error,
        )


class DaemonHealth(graphene.ObjectType):
    daemonStatus = graphene.Field(
        graphene.NonNull(DaemonStatus), daemon_type=graphene.Argument(DaemonType)
    )
    allDaemonStatuses = non_null_list(DaemonStatus)

    def __init__(self, instance):
        from dagster.daemon.controller import get_daemon_status

        self._daemon_statuses = {
            DaemonType.SCHEDULER.value: get_daemon_status(  # pylint: disable=no-member
                instance, DaemonType.SCHEDULER
            ),
            DaemonType.SENSOR.value: get_daemon_status(  # pylint: disable=no-member
                instance, DaemonType.SENSOR
            ),
            DaemonType.QUEUED_RUN_COORDINATOR.value: get_daemon_status(  # pylint: disable=no-member
                instance, DaemonType.QUEUED_RUN_COORDINATOR
            ),
        }

    def resolve_daemonStatus(self, _graphene_info, daemon_type):
        check.str_param(daemon_type, "daemon_type")  # DaemonType
        return DaemonStatus(self._daemon_statuses[daemon_type])

    def resolve_allDaemonStatuses(self, graphene_info):
        return [DaemonStatus(daemon_status) for daemon_status in self._daemon_statuses.values()]


class Instance(graphene.ObjectType):
    info = graphene.NonNull(graphene.String)
    runLauncher = graphene.Field(RunLauncher)
    assetsSupported = graphene.NonNull(graphene.Boolean)
    executablePath = graphene.NonNull(graphene.String)
    daemonHealth = graphene.NonNull(DaemonHealth)

    def __init__(self, instance):
        self._instance = check.inst_param(instance, "instance", DagsterInstance)

    def resolve_info(self, _graphene_info):
        return self._instance.info_str()

    def resolve_runLauncher(self, _graphene_info):
        return RunLauncher(self._instance.run_launcher) if self._instance.run_launcher else None

    def resolve_assetsSupported(self, _graphene_info):
        return self._instance.is_asset_aware

    def resolve_executablePath(self, _graphene_info):
        return sys.executable

    def resolve_daemonHealth(self, _graphene_info):
        return DaemonHealth(instance=self._instance)
