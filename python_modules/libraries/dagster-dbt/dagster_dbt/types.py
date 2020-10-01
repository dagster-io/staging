from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from dateutil import parser

from dagster import check, usable_as_dagster_type


class StepTiming:
    """The timing of an executed step for a dbt node (model).

    Note: Instances of :class:`StepTiming <dagster_dbt.StepTiming>` are intended to be constructed
    from JSON output from dbt.

    Example:
        .. code-block:: python

            json_data = {
                "name": "execute",
                "started_at": "2020-09-28T17:01:27.496435Z",
                "completed_at": "2020-09-28T17:01:27.590997Z"
            }

            step_timing = StepTiming(**json_data)
    """

    def __init__(self, name: str, started_at: str, completed_at: str, *args, **kwargs):
        """Constructor

        Args:
            name (str): The name of the executed step.
            started_at (str): An ISO string timestamp of when the step started executing.
            completed_at (str): An ISO string timestamp of when the step completed execution.
        """
        check.str_param(name, "name")
        check.str_param(started_at, "started_at")
        check.str_param(completed_at, "completed_at")

        self._name = name
        self._started_at = parser.isoparse(started_at)
        self._completed_at = parser.isoparse(completed_at)

    @property
    def name(self) -> str:
        """str: The name of the step in the executed dbt node (model)."""
        return self._name

    @property
    def started_at(self) -> datetime:
        """:obj:`datetime.datetime`: A timestamp of when the step started executing."""
        return self._started_at

    @property
    def completed_at(self) -> datetime:
        """:obj:`datetime.datetime`: A timestamp of when the step completed execution."""
        return self._completed_at

    @property
    def duration(self) -> timedelta:
        """:obj:`datetime.timedelta`: The execution duration of the step."""
        return self._completed_at - self._started_at


class NodeResult:
    """The result of executing a dbt node (model).

    Note: Instance of :class:`NodeResult <dagster_dbt.NodeResult>` are intended to be constructed
    from JSON output from dbt.
    """

    def __init__(
        self,
        node: Dict[str, Any],
        error: Optional[str],
        status: Union[str, int],
        execution_time: float,
        thread_id: str,
        step_timings: List[Dict[str, Any]],
        table: Optional[Dict[str, Any]] = None,
        fail: Optional[Any] = None,
        warn: Optional[Any] = None,
        skip: Optional[Any] = None,
        *args,
        **kwargs,
    ):
        """Constructor

        Args:
            node (Dict): Details about the executed dbt node (model).
            error (Optional[str]): An error message if an error occurred.
            fail (Optional[Any]): The ``fail`` field from the results of the executed dbt node.
            warn (Optional[Any]): The ``warn`` field from the results of the executed dbt node.
            skip (Optional[Any]): The ``skip`` field from the results of the executed dbt node.
            status (str | int): The status of the executed dbt node (model).
            execution_time (float): The execution duration (in seconds) of the dbt node (model).
            thread_id (str): The dbt thread identifier that executed the dbt node (model).
            step_timings (List[Dict[str, Any]]): The timings for each step in the executed dbt node
                (model).
            table (Optional[Dict]): Details about the table/view that is created from executing a
                `run_sql <https://docs.getdbt.com/reference/commands/rpc#executing-a-query>`_
                command on an dbt RPC server.
        """
        check.dict_param(node, "node", key_type=str)
        check.opt_str_param(error, "error")
        check.float_param(execution_time, "execution_time")
        check.str_param(thread_id, "thread_id")
        check.list_param(step_timings, "step_timings", of_type=Dict)
        check.opt_list_param(table, "table", of_type=Dict[str, Any])

        self._node = node
        self._error = error
        self._status = status
        self._execution_time = execution_time
        self._thread_id = thread_id
        self._step_timings = [StepTiming(**st) for st in step_timings]
        self._table = table
        self._fail = fail
        self._warn = warn
        self._skip = skip

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'NodeResult':
        """Constructs an instance of :class:`NodeResult <dagster_dbt.NodeResult>` from a dictionary.

        Args:
            d (Dict[str, Any]): a dictionary with key-values to construct a :class:`NodeResult
                <dagster_dbt.NodeResult>`.

        Returns:
            NodeResult: an instance of :class:`NodeResult <dagster_dbt.NodeResult>`.
        """
        check.dict_elem(d, "node")
        check.opt_str_elem(d, "error")
        # check.float_elem(d, "execution_time") TODO[Bob]: Implement `check.float_elem`.
        check.str_elem(d, "thread_id")
        check.list_elem(d, "timing")
        check.is_list(d["timing"], of_type=Dict)
        check.opt_dict_elem(d, "table")

        return cls(step_timings=d.get("timing"), **d)

    @property
    def node(self) -> Dict[str, Any]:
        """Dict[str, Any]: Details about the executed dbt node (model)."""
        return self._node

    @property
    def error(self) -> Optional[str]:
        """Optional[str]: An error message if an error occurred."""
        return self._error

    @property
    def status(self) -> Union[str, int]:
        """str | int: The status of the executed dbt node (model)."""
        return self._status

    @property
    def execution_time(self):
        """float: The execution duration (in seconds) of the dbt node (model)."""
        return self._execution_time

    @property
    def thread_id(self) -> str:
        """str: The dbt thread identifier that executed dbt node (model)."""
        return self._status

    @property
    def step_timings(self) -> List[StepTiming]:
        """List[StepTiming]: A list of :class:`StepTiming <dagster_dbt.StepTiming>`s for each step
        in the executed dbt node (model)."""
        return self._step_timings

    @property
    def table(self) -> Optional[Dict[str, Any]]:
        """Optional[Dict[str, Any]]: Details about the table/view that was created by the executed
        dbt node (model). This field is populated when running the `compile_sql
        <https://docs.getdbt.com/reference/commands/rpc#executing-a-query>`_ method on a dbt RPC
        server.
        """
        return self._table

    @property
    def fail(self) -> Optional[Any]:
        """Optional[Any]: The ``fail`` field from the results of the executed dbt node."""
        return self._fail

    @property
    def warn(self) -> Optional[Any]:
        """Optional[Any]: The ``warn`` field from the results of the executed dbt node."""
        return self._warn

    @property
    def skip(self) -> Optional[Any]:
        """Optional[Any]: The ``skip`` field from the results of the executed dbt node."""
        return self._skip


class RunResult:
    """The results of executing a dbt command.

    We recommend that you construct an instance of :class:`RunResult <dagster_dbt.RunResult>` by
    using the class method :func:`from_dict <dagster_dbt.RunResult.from_dict>`.

    When using the dbt CLI, run results are typically parsed from ``target/run_results.json``.
    """

    def __init__(
        self,
        logs: List[Dict[str, Any]],
        results: List[Dict[str, Any]],
        generated_at: str,
        elapsed_time: float,
        *args,
        **kwargs,
    ):
        """Constructor

        Args:
            results (List[Dict[str, Any]]): Details about each executed dbt node (model) in the run.
            generated_at (str): An ISO string timestamp of when the run result was generated by dbt.
            elapsed_time (flaot): The execution duration (in seconds) of the run.
        """
        check.list_param(logs, "logs", of_type=Dict)
        check.list_param(results, "results", of_type=Dict)
        check.str_param(generated_at, "generated_at")
        check.float_param(elapsed_time, "elapsed_time")

        self._logs = logs
        self._results = [NodeResult.from_dict(d) for d in results]
        self._generated_at = parser.isoparse(generated_at)
        self._elapsed_time = elapsed_time

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'RunResult':
        """Constructs an instance of :class:`RunResult <dagster_dbt.RunResult>` from a dictionary.

        Args:
            d (Dict[str, Any]): a dictionary with key-values to construct a :class:`RunResult
                <dagster_dbt.RunResult>`.

        Returns:
            RunResult: an instance of :class:`RunResult <dagster_dbt.RunResult>`.
        """
        check.list_elem(d, "logs")
        check.is_list(d["logs"], of_type=Dict)
        check.list_elem(d, "results")
        check.is_list(d["results"], of_type=Dict)
        check.str_elem(d, "generated_at")
        # check.float_elem(d, "elapsed_time") TODO[Bob]: Implement `check.float_elem`.

        return cls(**d)

    @property
    def results(self) -> List[NodeResult]:
        """List[NodeResult]: Details about each dbt node (model) that was executed in the run."""
        return self._results

    @property
    def generated_at(self) -> datetime:
        """datetime.datetime: A timestamp of when the run result was generated."""
        return self._generated_at

    @property
    def elapsed_time(self) -> float:
        """float: The execution duration (in seconds) of the run."""
        return self._elapsed_time

    def __len__(self) -> int:
        return len(self._results)

    def __getitem__(self, position) -> NodeResult:
        return self._results[position]
