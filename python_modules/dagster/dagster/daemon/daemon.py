from abc import abstractmethod

from dagster import DagsterInstance, check
from dagster.scheduler import execute_scheduler_iteration


class DagsterDaemon(object):
    def __init__(self, instance, logger, interval_seconds):
        self._instance = check.inst_param(instance, "instance", DagsterInstance)
        self._logger = logger
        self.interval_seconds = check.int_param(interval_seconds, "interval_seconds")
        self.last_iteration_time = None

    @abstractmethod
    def run_iteration(self):
        pass


class SchedulerDaemon(DagsterDaemon):
    def __init__(self, instance, logger, interval_seconds, max_catchup_runs):
        super(SchedulerDaemon, self).__init__(instance, logger, interval_seconds)
        self._max_catchup_runs = max_catchup_runs

    def run_iteration(self):
        execute_scheduler_iteration(self._instance, self._logger, self._max_catchup_runs)
