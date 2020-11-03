from abc import abstractmethod

from dagster.scheduler import execute_scheduler_loop


class DagsterDaemon(object):
    @abstractmethod
    def run(self):
        pass


class SchedulerDaemon(DagsterDaemon):
    def __init__(self, instance, logger, max_catchup_runs):
        self._instance = instance
        self._logger = logger
        self._max_catchup_runs = max_catchup_runs

    def run(self):
        execute_scheduler_loop(self._instance, self._logger, self._max_catchup_runs)
