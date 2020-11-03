from abc import abstractmethod

from dagster.scheduler import execute_scheduler_iteration


class DagsterDaemon(object):
    @abstractmethod
    def run_iteration(self):
        pass


class SchedulerDaemon(DagsterDaemon):
    def __init__(self, instance, logger, max_catchup_runs):
        self._instance = instance
        self._logger = logger
        self._max_catchup_runs = max_catchup_runs

    def run_iteration(self):
        execute_scheduler_iteration(self._instance, self._logger, self._max_catchup_runs)
