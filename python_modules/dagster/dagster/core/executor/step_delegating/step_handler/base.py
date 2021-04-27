import abc
from typing import List

from dagster import DagsterEvent
from dagster.core.execution.context.system import IStepContext, PlanOrchestrationContext
from dagster.core.execution.plan.state import KnownExecutionState
from dagster.core.execution.retries import RetryMode
from dagster.core.instance import DagsterInstance


class StepHandler(abc.ABC):  # pylint: disable=no-init
    def __init__(self, instance: DagsterInstance, retries: RetryMode):
        self._instance = instance
        self._retries = retries

    def initialize_for_execution(self, pipeline_context: PlanOrchestrationContext):
        self._pipeline_context = pipeline_context  # pylint: disable=attribute-defined-outside-init
        self._event_cursor = -1  # pylint: disable=attribute-defined-outside-init

    @abc.abstractproperty
    def name(self) -> str:
        pass

    @property
    def retries(self) -> RetryMode:
        return self._retries

    def pop_events(self) -> List[DagsterEvent]:
        events = self._instance.logs_after(
            self._pipeline_context.pipeline_run.run_id, self._event_cursor
        )
        self._event_cursor += len(events)
        return [event.dagster_event for event in events if event.is_dagster_event]

    @abc.abstractmethod
    def launch_steps(
        self,
        pipeline_context: PlanOrchestrationContext,
        step_contexts: List[IStepContext],
        known_state: KnownExecutionState,
    ):
        pass

    @abc.abstractmethod
    def check_step_health(
        self,
        pipeline_context: PlanOrchestrationContext,
        step_contexts: List[IStepContext],
        known_state: KnownExecutionState,
    ) -> List[DagsterEvent]:
        pass

    @abc.abstractmethod
    def terminate_steps(self, step_keys: List[str]):
        pass
