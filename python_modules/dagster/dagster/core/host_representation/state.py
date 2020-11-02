from collections import namedtuple
from enum import Enum

from dagster import check


class HandleStateChangeEventType(Enum):
    SERVER_UPDATED = "SERVER_UPDATED"
    SERVER_RECONNECTING = "SERVER_RECONNECTING"
    SERVER_RECONNECTED = "SERVER_RECONNECTED"
    SERVER_ERROR = "SERVER_ERROR"


class HandleStateChangeEvent(
    namedtuple("_HandleStateChangeEvent", "event_type message location_name")
):
    def __new__(cls, event_type, message, location_name):
        return super(HandleStateChangeEvent, cls).__new__(
            cls,
            event_type,
            check.str_param(message, "message"),
            check.str_param(location_name, "location_name"),
        )


class HandleStateChangeSubscriber(object):
    def __init__(self, callback):
        check.callable_param(callback, "callback")

        self._events = []
        self._callback = callback

    def handle_event(self, event):
        check.inst_param(event, "event", HandleStateChangeEvent)
        self._callback(event)
