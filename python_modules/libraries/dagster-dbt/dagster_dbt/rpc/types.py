from datetime import datetime, timedelta
from typing import Any, Dict

import pytz
from dateutil import parser

from dagster import check, usable_as_dagster_type

from ..types import RunResult


@usable_as_dagster_type
class RpcRunResult(RunResult):
    """The results of executing a dbt command, along with additional metadata about the dbt process
    that was run on the dbt RPC server.

    We recommend that you construct an instance of :class:`RpcRunResult
    <dagster_dbt.RpcRunResult>` by using the class method:func:`from_dict
    <dagster_dbt.RpcRunResult.from_dict>`.

    When using the dbt RPC server, polled run results are typically parsed from the JSON body of
    the RPC response.
    """

    def __init__(self, *args, state: str, start: str, end: str, elapsed: float, **kwargs):
        """Constructor

        Args:
            state (str): The state of the polled dbt process.
            start (str): An ISO string timestamp of when the dbt process started.
            end (str): An ISO string timestamp of when the dbt process ended.
            elapsed (float): The duration (in seconds) for which the dbt process was running.
        """
        super().__init__(*args, **kwargs)

        check.str_param(state, "state")
        check.str_param(start, "start")
        check.str_param(end, "end")
        check.float_param(elapsed, "elapsed")

        self._state = state
        self._start = parser.isoparse(start)
        self._end = parser.isoparse(end)
        self._elapsed = elapsed

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'RpcRunResult':
        """Constructs an instance of :class:`RpcRunResult <dagster_dbt.RpcRunResult>` from a
        dictionary.

        Args:
            d (Dict[str, Any]): a dictionary with key-values to construct a :class:`RpcRunResult
                <dagster_dbt.RpcRunResult>`.

        Returns:
            RpcRunResult: an instance of :class:`RpcRunResult <dagster_dbt.RpcRunResult>`.
        """
        check.str_elem(d, "state")
        check.str_elem(d, "start")
        check.str_elem(d, "end")
        # check.float_elem(d, "elapsed") TODO[Bob]: Impelement `check.float_elem`.

        RunResult.from_dict(d)  # Run the type checks for parent class, but throw away the instance.

        return cls(**d)

    @property
    def state(self) -> str:
        """str: The state of the polled dbt process."""
        return self._state

    @property
    def start(self) -> datetime:
        """datetime.datetime: A timestamp of when the dbt process started."""
        return self._start

    @property
    def end(self) -> datetime:
        """datetime.datetime: A timestamp of when the dbt process ended."""
        return self._end

    @property
    def elapsed(self) -> float:
        """float: The duration (in seconds) for which the dbt process was running."""
        return self._elapsed
