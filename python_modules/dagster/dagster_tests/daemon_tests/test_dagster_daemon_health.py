import datetime

import pendulum
from dagster import DagsterInvariantViolationError
from dagster.core.test_utils import instance_for_test
from dagster.daemon.controller import (
    DagsterDaemonController,
    all_daemons_healthy,
    get_daemon_status,
)


def test_healthy():

    with instance_for_test(
        overrides={
            "run_coordinator": {
                "module": "dagster.core.run_coordinator.queued_run_coordinator",
                "class": "QueuedRunCoordinator",
            },
        }
    ) as instance:
        init_time = pendulum.now("UTC")
        beyond_tolerated_time = init_time + datetime.timedelta(seconds=100)

        controller = DagsterDaemonController(instance)
        assert not all_daemons_healthy(instance, curr_time=init_time)

        controller.run_iteration(init_time)
        assert all_daemons_healthy(instance, curr_time=init_time)

        assert not all_daemons_healthy(instance, curr_time=beyond_tolerated_time)


def test_healthy_with_different_daemons():
    with instance_for_test() as instance:
        init_time = pendulum.now("UTC")
        controller = DagsterDaemonController(instance)
        controller.run_iteration(init_time)

    with instance_for_test(
        overrides={
            "run_coordinator": {
                "module": "dagster.core.run_coordinator.queued_run_coordinator",
                "class": "QueuedRunCoordinator",
            },
        }
    ) as instance:
        assert not all_daemons_healthy(instance, curr_time=init_time)


def test_error_daemon(monkeypatch):
    with instance_for_test(overrides={}) as instance:
        from dagster.daemon.daemon import SensorDaemon

        def run_iteration_error(_):
            raise DagsterInvariantViolationError("foobar")

        monkeypatch.setattr(SensorDaemon, "run_iteration", run_iteration_error)
        controller = DagsterDaemonController(instance)
        init_time = pendulum.now("UTC")
        controller.run_iteration(init_time)

        status = get_daemon_status(instance, SensorDaemon.daemon_type(), init_time)
        assert status.healthy == False
        assert status.last_heartbeat.info.error == "foobar"
