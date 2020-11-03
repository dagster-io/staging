import re

import pytest
from dagster.core.test_utils import instance_for_test
from dagster.daemon.controller import DagsterDaemonController
from dagster.daemon.daemon import SchedulerDaemon
from dagster.daemon.run_coordinator.queued_run_coordinator_daemon import QueuedRunCoordinatorDaemon


def test_empty_instance():
    with instance_for_test() as instance:
        with pytest.raises(
            Exception, match=re.escape("No daemons configured on the DagsterInstance")
        ):
            DagsterDaemonController(instance)


def test_scheduler_instance():
    with instance_for_test(
        overrides={
            "scheduler": {"module": "dagster.core.scheduler", "class": "DagsterDaemonScheduler",},
        }
    ) as instance:
        controller = DagsterDaemonController(instance)

        daemons = controller._daemons  # pylint: disable=protected-access

        assert len(daemons) == 1
        assert isinstance(daemons[0], SchedulerDaemon)


def test_run_coordinator_instance():
    with instance_for_test(
        overrides={
            "run_coordinator": {
                "module": "dagster.core.run_coordinator.queued_run_coordinator",
                "class": "QueuedRunCoordinator",
            },
        }
    ) as instance:
        controller = DagsterDaemonController(instance)

        daemons = controller._daemons  # pylint: disable=protected-access

        assert len(daemons) == 1
        assert isinstance(daemons[0], QueuedRunCoordinatorDaemon)
