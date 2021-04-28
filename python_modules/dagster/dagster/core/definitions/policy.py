from typing import NamedTuple, Optional

from dagster import check


class RetryPolicy(
    NamedTuple(
        "_RetryPolicy",
        [
            ("max_retries", int),
            ("seconds_to_wait", Optional[check.Numeric]),
            # delay control must be declarative - can't run a random call back from host process orchestrator
        ],
    ),
):
    def __new__(cls, max_retries=1, seconds_to_wait=None):
        return super().__new__(
            cls,
            max_retries=check.int_param(max_retries, "max_retries"),
            seconds_to_wait=check.opt_numeric_param(seconds_to_wait, "seconds_to_wait"),
        )
