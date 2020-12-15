from collections import namedtuple
from datetime import datetime
from enum import Enum

from dagster import check
from dagster.serdes import whitelist_for_serdes


class DaemonType(Enum):
    """
    Note: string values are used for serialization into db
    """

    SENSOR = "SENSOR"
    SCHEDULER = "SCHEDULER"
    QUEUED_RUN_COORDINATOR = "QUEUED_RUN_COORDINATOR"


@whitelist_for_serdes
class DaemonHeartbeatInfo(namedtuple("_DaemonHeartbeatInfo", "error")):
    """
    Heartbeat info holds additional fields on heartbeats that are serialized into a single column
    in storage.
    """

    def __new__(cls, error):
        check.opt_str_param(error, "error")
        return super(DaemonHeartbeatInfo, cls).__new__(cls, error)


@whitelist_for_serdes
class DaemonHeartbeat(namedtuple("_DaemonHeartbeat", "timestamp daemon_type daemon_id info")):
    """
    Heartbeats are placed in storage by the daemon to show liveness
    """

    def __new__(cls, timestamp, daemon_type, daemon_id, info):
        check.inst_param(timestamp, "timestamp", datetime)
        check.inst_param(daemon_type, "daemon_type", DaemonType)
        check.opt_inst_param(info, "info", DaemonHeartbeatInfo)
        return super(DaemonHeartbeat, cls).__new__(cls, timestamp, daemon_type, daemon_id, info)


class DaemonStatus(namedtuple("_DaemonStatus", "daemon_type required healthy last_heartbeat")):
    """
    Daemon statuses are derived from daemon heartbeats and instance configuration to provide an
    overview about the daemon's liveness.
    """

    def __new__(cls, daemon_type, required, healthy, last_heartbeat):
        check.inst_param(daemon_type, "daemon_type", DaemonType)
        check.bool_param(required, "required")
        check.opt_bool_param(healthy, "healthy")
        check.opt_inst_param(last_heartbeat, "last_heartbeat", DaemonHeartbeat)
        return super(DaemonStatus, cls).__new__(cls, daemon_type, required, healthy, last_heartbeat)
