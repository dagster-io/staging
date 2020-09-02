import json
import os
import re
import signal
import subprocess
from typing import List

import attr

from dagster import check, usable_as_dagster_type

from ..errors import DagsterDbtFatalCliRuntimeError, DagsterDbtHandledCliRuntimeError


@usable_as_dagster_type
@attr.s
class DbtCliResult:
    logs: List[dict] = attr.ib()
    return_code: int = attr.ib()  # https://docs.getdbt.com/reference/exit-codes/
    raw_output: str = attr.ib()

    n_pass: int = attr.ib(default=None, converter=attr.converters.optional(int))
    n_warn: int = attr.ib(default=None, converter=attr.converters.optional(int))
    n_error: int = attr.ib(default=None, converter=attr.converters.optional(int))
    n_skip: int = attr.ib(default=None, converter=attr.converters.optional(int))
    n_total: int = attr.ib(default=None, converter=attr.converters.optional(int))


def pre_exec():
    # Restore default signal disposition and invoke setsid
    for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
        if hasattr(signal, sig):
            signal.signal(getattr(signal, sig), signal.SIG_DFL)
    os.setsid()


def execute_dbt(executable, command, args_dict, log, ignore_handled_error) -> DbtCliResult:
    check.str_param(command, "command")

    if "vars" in args_dict:
        check.dict_param(args_dict["vars"], "args.vars", key_type=str, value_type=str)
        args_dict["vars"] = json.dumps(args_dict)

    check.dict_param(args_dict, "args_dict", key_type=str, value_type=str)

    command_list = [executable, "--log-format", "json", command]
    for flag, value in args_dict.items():
        command_list.append(f"--{flag}")
        if value != True:  # if a bool flag, the flag itself is enough
            command_list.append(value)

    return_code = 0
    try:
        proc_out = subprocess.check_output(
            command_list, preexec_fn=pre_exec, stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError as exc:
        return_code = exc.returncode
        proc_out = exc.output

    parsed_output = []
    for raw_line in proc_out.strip().split(b"\n"):
        line = raw_line.decode()
        log.info(line.rstrip())
        try:
            json_line = json.loads(line)
            parsed_output.append(json_line)
        except json.JSONDecodeError:
            pass

    log.info("dbt exited with return code {retcode}".format(retcode=return_code))

    if return_code == 2:
        raise DagsterDbtFatalCliRuntimeError(
            parsed_output=parsed_output, raw_output=proc_out.decode()
        )

    if return_code == 1:
        if ignore_handled_error:
            return DbtCliResult(
                logs=parsed_output, raw_output=proc_out.decode(), return_code=return_code
            )

        raise DagsterDbtHandledCliRuntimeError(
            parsed_output=parsed_output, raw_output=proc_out.decode()
        )

    result_stats = get_results(parsed_output)

    return DbtCliResult(
        logs=parsed_output, raw_output=proc_out.decode(), return_code=return_code, **result_stats,
    )


RESULT_STATS_RE = re.compile(r"PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+) TOTAL=(\d+)")
RESULT_STATS_LABELS = ("n_pass", "n_warn", "n_error", "n_skip", "n_total")


def get_results(parsed_output):
    check.list_param(parsed_output, "parsed_output", dict)

    last_line = parsed_output[-1]
    message = last_line["message"].strip()

    try:
        matches = next(RESULT_STATS_RE.finditer(message)).groups()
    except StopIteration:
        matches = [None] * 5  # TODO defensive edge case?

    return dict(zip(RESULT_STATS_LABELS, matches))
