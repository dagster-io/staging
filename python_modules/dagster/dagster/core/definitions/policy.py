from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, NamedTuple

from dagster import check

from .events import RetryRequested

if TYPE_CHECKING:
    from dagster.core.execution.context.system import StepExecutionContext


class SolidExecutionPolicy(ABC):
    """
    A declarative specification about what to do when a solid executes.
    """

    @abstractmethod
    def on_exception(self, step_context: "StepExecutionContext", exc: Exception):
        pass


def _none(_):
    return None


class RetryPolicy(
    NamedTuple(
        "_RetryPolicy",
        [
            ("max_retries", int),
            ("seconds_to_wait_fn", Callable[[int], int]),
        ],
    ),
    SolidExecutionPolicy,
):
    def __new__(cls, max_retries=1, seconds_to_wait_fn=None):
        return super().__new__(
            cls,
            max_retries=check.int_param(max_retries, "max_retries"),
            seconds_to_wait_fn=check.opt_callable_param(
                seconds_to_wait_fn,
                "seconds_to_wait_fn",
                default=_none,
            ),
        )

    def on_exception(self, step_context: "StepExecutionContext", exc: Exception):
        # could check exc against a whitelist of exceptions
        raise RetryRequested(
            max_retries=self.max_retries,
            # is a function to allow for custom backoff
            seconds_to_wait=self.seconds_to_wait_fn(step_context.previous_attempt_count),
        ) from exc
