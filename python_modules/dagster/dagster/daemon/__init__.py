import logging
import sys
import time
from abc import abstractmethod

import pendulum
from dagster.core.run_coordinator import QueuedRunCoordinator
from dagster.core.scheduler import DagsterDaemonScheduler
from dagster.scheduler import execute_scheduler_loop
from dagster.utils.log import default_format_string

from .run_coordinator.queued_run_coordinator_daemon import QueuedRunCoordinatorDaemon


def _mockable_localtime(_):
    now_time = pendulum.now()
    return now_time.timetuple()


def get_default_daemon_logger():
    handler = logging.StreamHandler(sys.stdout)
    logger = logging.getLogger("dagster-scheduler")
    logger.setLevel(logging.INFO)
    logger.handlers = [handler]

    formatter = logging.Formatter(default_format_string(), "%Y-%m-%d %H:%M:%S")

    formatter.converter = _mockable_localtime

    handler.setFormatter(formatter)
    return logger


def _sorted_quoted(strings):
    return "[" + ", ".join(["'{}'".format(s) for s in sorted(list(strings))]) + "]"


class DagsterDaemon(object):
    @abstractmethod
    def run(self):
        pass


class DagsterDaemonController(object):
    def __init__(self, instance):
        self._instance = instance

        self._daemons = []

        self._logger = get_default_daemon_logger()

        # Should changing instance config (changing the scheduler/run coordinator/run launcher)
        # require a daemon process restart/re-deploy?

        if isinstance(instance.scheduler, DagsterDaemonScheduler):
            # Should we determine parameters of the daemons from CLI args or instance config??
            # For now assuming the latter
            max_catchup_runs = instance.scheduler.max_catchup_runs
            self._daemons.append(SchedulerDaemon(instance, self._logger, max_catchup_runs))

        if isinstance(instance.run_coordinator, QueuedRunCoordinator):
            max_concurrent_runs = instance.run_coordinator.max_concurrent_runs
            self._daemons.append(QueuedRunCoordinatorDaemon(instance, max_concurrent_runs))

        if not self._daemons:
            raise Exception("No daemons configured on the DagsterInstance")

        self._logger.info(
            "instance is configured with the following daemons: {}".format(
                _sorted_quoted(daemon.__name__ for daemon in self._daemons)
            )
        )

    def run(self, interval_seconds):
        while True:
            start_time = pendulum.now("UTC")

            for daemon in self._daemons:
                daemon.run()

            time_left = interval_seconds - (pendulum.now("UTC") - start_time).seconds
            if time_left > 0:
                time.sleep(time_left)


class SchedulerDaemon(DagsterDaemon):
    def __init__(self, instance, logger, max_catchup_runs):
        self._instance = instance
        self._logger = logger
        self._max_catchup_runs = max_catchup_runs

    def run(self):
        execute_scheduler_loop(self._instance, self._logger, self._max_catchup_runs)
