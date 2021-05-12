import abc
from typing import List

from dagster import DagsterEvent
from dagster.core.execution.context.system import IStepContext, PlanOrchestrationContext
from dagster.core.execution.plan.state import KnownExecutionState
from dagster.core.execution.retries import RetryMode
from dagster.grpc.types import ExecuteStepArgs


class StepHandler(abc.ABC):  # pylint: disable=no-init
    def __init__(self, retries: RetryMode):
        self._retries = retries

    @abc.abstractproperty
    def name(self) -> str:
        pass

    @property
    def retries(self) -> RetryMode:
        return self._retries

    @abc.abstractmethod
    def launch_step(
        self,
        execute_step_args: ExecuteStepArgs,
    ):
        pass

    @abc.abstractmethod
    def check_step_health(
        self,
        execute_step_args: ExecuteStepArgs,
    ) -> List[DagsterEvent]:
        pass

    @abc.abstractmethod
    def terminate_step(
        self,
        execute_step_args: ExecuteStepArgs,
    ):
        pass
