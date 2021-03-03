from collections import defaultdict
from enum import Enum

from dagster import Field, Selector, check


def get_retries_config():
    return Field(
        Selector({"enabled": {}, "disabled": {}}),
        is_required=False,
        default_value={"enabled": {}},
    )


class RetryMode(Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    # Designed for use of inner plan execution within "orchestrator" engine such as multiprocess,
    # up_for_retry steps are not directly re-enqueued, deferring that to the engine.
    DEFERRED = "deferred"

    @staticmethod
    def from_config(config_value):
        for selector, _ in config_value.items():
            return RetryMode(selector)

    @property
    def enabled(self):
        return self == RetryMode.ENABLED

    @property
    def disabled(self):
        return self == RetryMode.DISABLED

    @property
    def deferred(self):
        return self == RetryMode.DEFERRED

    def for_inner_plan(self):
        if self.disabled or self.deferred:
            return self
        elif self.enabled:
            return RetryMode.DEFERRED
        else:
            check.failed("Unexpected RetryMode! Expected enabled, disabled, or deferred")


class RetryState:
    def __init__(self, previous_attempts=None):
        self._attempts = defaultdict(int)
        for key, val in check.opt_dict_param(
            previous_attempts, "previous_attempts", key_type=str, value_type=int
        ).items():
            self._attempts[key] = val

    def get_attempt_count(self, key):
        return self._attempts[key]

    def mark_attempt(self, key):
        self._attempts[key] += 1

    def to_dict(self):
        return {"previous_retry_attempts": dict(self._attempts)}
