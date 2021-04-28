from typing import NamedTuple, Optional

from dagster import check
from dagster.utils.backcompat import experimental_class_warning


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
    """
    A declarative policy for when to request retries when an exception occurs during solid execution.

    Args:
        max_retries (int):
            The maximum number of retries to attempt. Defaults to 1.
        seconds_to_wait (Optional[Union[int,float]]):
            The time in seconds to wait between the retry being requested and the next attempt being started.

    """

    def __new__(cls, max_retries=1, seconds_to_wait=None):
        experimental_class_warning("RetryPolicy")
        return super().__new__(
            cls,
            max_retries=check.int_param(max_retries, "max_retries"),
            seconds_to_wait=check.opt_numeric_param(seconds_to_wait, "seconds_to_wait"),
        )
