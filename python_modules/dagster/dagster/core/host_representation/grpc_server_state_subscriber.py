from collections import namedtuple
from enum import Enum

from dagster import check


class GrpcServerStateChangeEventType(Enum):
    SERVER_UPDATED = "SERVER_UPDATED"
    SERVER_DISCONNECTED = "SERVER_DISCONNECTED"
    SERVER_RECONNECTED = "SERVER_RECONNECTED"
    SERVER_ERROR = "SERVER_ERROR"


class GrpcServerStateChangeEvent(
    namedtuple("_GrpcServerStateChangeEvent", "event_type location_name message server_id")
):
    def __new__(cls, event_type, location_name, message, server_id=None):
        return super(GrpcServerStateChangeEvent, cls).__new__(
            cls,
            check.inst_param(event_type, "event_type", GrpcServerStateChangeEventType),
            check.str_param(location_name, "location_name"),
            check.str_param(message, "message"),
            check.opt_str_param(server_id, "server_id"),
        )


class GrpcServerStateSubscriber(object):
    def __init__(self, callback):
        check.callable_param(callback, "callback")
        self._callback = callback

    def handle_event(self, event):
        check.inst_param(event, "event", GrpcServerStateChangeEvent)
        self._callback(event)
