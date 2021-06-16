from collections import namedtuple
from typing import Any, Dict, List, Optional

from dagster import check, usable_as_dagster_type

from ..types import DbtResult


@usable_as_dagster_type
class DbtCliOutput(
    namedtuple(
        "_DbtCliOutput",
        "command return_code raw_output logs result",
    ),
):
    """The results of executing a dbt command, along with additional metadata about the dbt CLI
    process that was run.

    Note that users should not construct instances of this class directly. This class is intended
    to be constructed from the JSON output of dbt commands.

    Attributes:
        command (str): The full shell command that was executed.
        return_code (int): The return code of the dbt CLI process.
        raw_output (str): The raw output (``stdout``) of the dbt CLI process.
        summary (Optional[Dict[str, Optional[int]]]): Dictionary containing counts of the number of
            dbt nodes that passed, failed, emitted warnings, and were skipped (if applicable).
        logs (List[Dict[str, Any]]): List of parsed JSON logs produced by the dbt command.
        result (Optional[DbtResult]): Parsed object containing dbt-reported result information. Some
            dbt commands do not produce results, and will therefore have result = None.
    """

    def __new__(
        cls,
        command: str,
        return_code: int,
        raw_output: str,
        logs: List[Dict[str, Any]],
        result: Optional[DbtResult] = None,
    ):
        return super().__new__(
            cls,
            command=check.str_param(command, "command"),
            return_code=check.int_param(return_code, "return_code"),
            raw_output=check.str_param(raw_output, "raw_output"),
            logs=check.list_param(logs, "logs", of_type=dict),
            result=check.opt_inst_param(result, "result", DbtResult),
        )
