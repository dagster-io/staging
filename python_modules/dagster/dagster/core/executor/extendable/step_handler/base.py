import abc

from dagster.core.instance import DagsterInstance
from dagster.grpc.types import ExecuteStepArgs


class StepHandler(abc.ABC):  # pylint: disable=no-init
    def initialize(self, instance: DagsterInstance, run_id: str):
        self._instance = instance
        self._run_id = run_id
        self._event_cursor = -1

    def pop_events(self):
        events = self._instance.logs_after(self._run_id, self._event_cursor)
        self._event_cursor += len(events)
        return events

    @abc.abstractmethod
    def launch_steps(self, execute_step_args: ExecuteStepArgs):
        pass
