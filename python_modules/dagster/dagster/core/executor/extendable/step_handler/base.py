import abc
from typing import List

from dagster.core.execution.retries import RetryMode
from dagster.core.instance import DagsterInstance


class StepHandler(abc.ABC):  # pylint: disable=no-init
    def __init__(self):
        self._instance = None
        self._pipeline_context = None
        self._retries = None
        self._run_id = None

        self._event_cursor = None

    def initialize(self, instance: DagsterInstance, pipeline_context, retries: RetryMode):
        self._instance = instance
        self._pipeline_context = pipeline_context
        self._retries = retries
        self._run_id = pipeline_context.pipeline_run.run_id

        self._event_cursor = -1

    @abc.abstractproperty
    def name(self):
        pass

    @property
    def retries(self):
        return self._retries

    def pop_events(self):
        events = self._instance.logs_after(self._run_id, self._event_cursor)
        self._event_cursor += len(events)
        return events

    @abc.abstractmethod
    def launch_steps(self, pipeline, step_context, step, known_state):
        pass

    @abc.abstractmethod
    def check_step_health(self, step):
        pass

    @abc.abstractmethod
    def terminate_steps(self, step_keys: List[str]):
        pass
