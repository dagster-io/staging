import uuid
from datetime import datetime

import pendulum
from dagster import check
from dagster.core.run_coordinator import QueuedRunCoordinator
from dagster.core.scheduler import DagsterDaemonScheduler
from dagster.daemon.daemon import SchedulerDaemon, SensorDaemon, get_default_daemon_logger
from dagster.daemon.run_coordinator.queued_run_coordinator_daemon import QueuedRunCoordinatorDaemon
from dagster.daemon.types import DaemonHeartbeat

DAEMON_HEARTBEAT_INTERVAL_SECONDS = 30
DAEMON_HEARTBEAT_TOLERANCE_SECONDS = 60


def _sorted_quoted(strings):
    return "[" + ", ".join(["'{}'".format(s) for s in sorted(list(strings))]) + "]"


class DagsterDaemonController:
    @staticmethod
    def required(instance):
        """
        True if the instance configuration has classes that require the daemon to be enabled.
        """
        return len(DagsterDaemonController._expected_daemons(instance)) > 0

    @staticmethod
    def _expected_daemons(instance):
        daemons = [SensorDaemon.__name__]
        if isinstance(instance.scheduler, DagsterDaemonScheduler):
            daemons.append(SchedulerDaemon.__name__)
        if isinstance(instance.run_coordinator, QueuedRunCoordinator):
            daemons.append(QueuedRunCoordinatorDaemon.__name__)
        return daemons

    @staticmethod
    def healthy(instance, curr_time):
        """
        True if the daemon process has had a recent heartbeat
        """
        check.opt_inst_param(curr_time, "curr_time", datetime)
        assert DagsterDaemonController.required(instance)
        heartbeat = instance.get_daemon_heartbeats().get("dagster-daemon", None)
        if not heartbeat:
            return False
        return pendulum.now() <= pendulum.instance(heartbeat.timestamp).add(
            seconds=DAEMON_HEARTBEAT_TOLERANCE_SECONDS
        )

    def __init__(self, instance):
        self._instance = instance

        self._daemon_uuid = str(uuid.uuid4())

        self._daemons = {}
        self._last_heartbeat_time = None

        self._logger = get_default_daemon_logger("dagster-daemon")

        if isinstance(instance.scheduler, DagsterDaemonScheduler):
            max_catchup_runs = instance.scheduler.max_catchup_runs
            self._add_daemon(
                SchedulerDaemon(instance, interval_seconds=30, max_catchup_runs=max_catchup_runs)
            )

        self._add_daemon(SensorDaemon(instance, interval_seconds=30))

        if isinstance(instance.run_coordinator, QueuedRunCoordinator):
            max_concurrent_runs = instance.run_coordinator.max_concurrent_runs
            dequeue_interval_seconds = instance.run_coordinator.dequeue_interval_seconds
            self._add_daemon(
                QueuedRunCoordinatorDaemon(
                    instance,
                    interval_seconds=dequeue_interval_seconds,
                    max_concurrent_runs=max_concurrent_runs,
                )
            )

        assert set(self._expected_daemons(instance)) == self._daemons.keys()

        if not self._daemons:
            raise Exception("No daemons configured on the DagsterInstance")

        self._logger.info(
            "instance is configured with the following daemons: {}".format(
                _sorted_quoted(type(daemon).__name__ for daemon in self.daemons)
            )
        )

    def _add_daemon(self, daemon):
        self._daemons[type(daemon).__name__] = daemon

    def get_daemon(self, daemon_type):
        return self._daemons.get(daemon_type)

    @property
    def daemons(self):
        return list(self._daemons.values())

    def run_iteration(self, curr_time):
        # Add a hearbeat to storage. Note that the time to call `run_iteration` on all the daemons
        # should not be near DAEMON_HEARTBEAT_TOLERANCE_SECONDS
        should_add_heartbeat = False
        if not self._last_heartbeat_time:
            should_add_heartbeat = True
        else:
            time_since_last_heartbeat = (curr_time - self._last_heartbeat_time).total_seconds()
            if time_since_last_heartbeat >= DAEMON_HEARTBEAT_INTERVAL_SECONDS:
                should_add_heartbeat = True

        if should_add_heartbeat:
            self.add_heartbeat()

        for daemon in self.daemons:
            if (not daemon.last_iteration_time) or (
                (curr_time - daemon.last_iteration_time).total_seconds() >= daemon.interval_seconds
            ):
                daemon.last_iteration_time = curr_time
                daemon.run_iteration()

    def add_heartbeat(self):
        self._instance.add_daemon_heartbeat(
            DaemonHeartbeat(pendulum.now("UTC"), "dagster-daemon", None, None)
        )
