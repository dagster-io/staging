import subprocess

from dagster import executor
from dagster.grpc.types import ExecuteStepArgs
from dagster.serdes.serdes import serialize_dagster_namedtuple

from ..core_executor import CoreExecutor
from .base import StepHandler


class MultiprocessStepHandler(StepHandler):
    def launch_steps(self, execute_step_args: ExecuteStepArgs):
        subprocess.Popen(
            args=[
                "dagster",
                "api",
                "execute_step",
                serialize_dagster_namedtuple(execute_step_args),
            ],
            stdout=None,
            stderr=subprocess.STDOUT,
            # cwd=DAGSTER_REPO,
        )


@executor(
    name="new-multiprocess",
    config_schema={},
)
def multiprocess_executor(_init_context):
    return CoreExecutor(MultiprocessStepHandler())
