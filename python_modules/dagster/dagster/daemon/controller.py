import logging
import sys
import time

import pendulum
from dagster.core.run_coordinator import QueuedRunCoordinator
from dagster.core.scheduler import DagsterDaemonScheduler
from dagster.daemon.daemon import SchedulerDaemon
from dagster.daemon.run_coordinator.queued_run_coordinator_daemon import QueuedRunCoordinatorDaemon
from dagster.utils.log import default_format_string


def _mockable_localtime(_):
    now_time = pendulum.now()
    return now_time.timetuple()


def get_default_daemon_logger():
    handler = logging.StreamHandler(sys.stdout)
    logger = logging.getLogger("dagster-daemon")
    logger.setLevel(logging.INFO)
    logger.handlers = [handler]

    formatter = logging.Formatter(default_format_string(), "%Y-%m-%d %H:%M:%S")

    formatter.converter = _mockable_localtime

    handler.setFormatter(formatter)
    return logger


def _sorted_quoted(strings):
    return "[" + ", ".join(["'{}'".format(s) for s in sorted(list(strings))]) + "]"


class DagsterDaemonController(object):
    def __init__(self, instance):
        self._instance = instance

        self._daemons = []

        self._logger = get_default_daemon_logger()

        if isinstance(instance.scheduler, DagsterDaemonScheduler):
            max_catchup_runs = instance.scheduler.max_catchup_runs
            self._daemons.append(
                SchedulerDaemon(
                    instance, self._logger, interval_seconds=30, max_catchup_runs=max_catchup_runs
                )
            )

        if isinstance(instance.run_coordinator, QueuedRunCoordinator):
            max_concurrent_runs = instance.run_coordinator.max_concurrent_runs
            dequeue_interval_seconds = instance.run_coordinator.dequeue_interval_seconds
            self._daemons.append(
                QueuedRunCoordinatorDaemon(
                    instance,
                    self._logger,
                    interval_seconds=dequeue_interval_seconds,
                    max_concurrent_runs=max_concurrent_runs,
                )
            )

        if not self._daemons:
            raise Exception("No daemons configured on the DagsterInstance")

        self._logger.info(
            "instance is configured with the following daemons: {}".format(
                _sorted_quoted(type(daemon).__name__ for daemon in self._daemons)
            )
        )

    def run(self):
        while True:
            curr_time = pendulum.now("UTC")

            for daemon in self._daemons:
                if (not daemon.last_iteration_time) or (
                    (curr_time - daemon.last_iteration_time).total_seconds()
                    >= daemon.interval_seconds
                ):
                    daemon.last_iteration_time = curr_time
                    daemon.run_iteration()

            time.sleep(0.5)
