import subprocess

from dagster import check
from dagster.serdes.ipc import DagsterIPCProtocolError, read_unary_response, write_unary_input
from dagster.utils.temp_file import get_temp_file_name


def execute_unary_api_cli_command(executable_path, command_name, input_obj):
    with get_temp_file_name() as input_file, get_temp_file_name() as output_file:
        parts = [
            executable_path,
            "-m",
            "dagster",
            "api",
            command_name,
            input_file,
            output_file,
        ]

        write_unary_input(input_file, input_obj)

        subprocess.check_call(parts)

        return read_unary_response(output_file)
