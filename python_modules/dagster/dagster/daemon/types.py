from collections import namedtuple


class DaemonHeartbeat(namedtuple("_DaemonHeartbeat", "timestamp daemon_type daemon_id info")):
    def __new__(cls, timestamp, daemon_type, daemon_id, info):
        return super(DaemonHeartbeat, cls).__new__(cls, timestamp, daemon_type, daemon_id, info)


class DaemonStatus(namedtuple("_DaemonStatus", "required healthy last_heartbeat")):
    def __new__(cls, required, healthy, last_heartbeat):
        return super(DaemonStatus, cls).__new__(cls, required, healthy, last_heartbeat)
