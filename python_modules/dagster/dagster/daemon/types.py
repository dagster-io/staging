from collections import namedtuple

from dagster import check


class DaemonHeartbeat(namedtuple("_DaemonHeartbeat", "timestamp daemon_type daemon_id info")):
    def __new__(cls, timestamp, daemon_type, daemon_id, info):
        return super(DaemonHeartbeat, cls).__new__(cls, timestamp, daemon_type, daemon_id, info)


class DaemonStatus(namedtuple("_DaemonStatus", "daemon_type required healthy last_heartbeat")):
    def __new__(cls, daemon_type, required, healthy, last_heartbeat):
        check.str_param(daemon_type, "daemon_type")
        check.bool_param(required, "required")
        check.opt_bool_param(healthy, "healthy")
        check.opt_inst_param(last_heartbeat, "last_heartbeat", DaemonHeartbeat)
        return super(DaemonStatus, cls).__new__(cls, daemon_type, required, healthy, last_heartbeat)
