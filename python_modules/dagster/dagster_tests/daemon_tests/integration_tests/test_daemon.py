import os
import subprocess
import time

import pytest
from dagster import DagsterInstance


@pytest.fixture
def monkeypatch_daemon_heartbeat_timing(monkeypatch):
    monkeypatch.setattr("dagster.daemon.controller.DAEMON_HEARTBEAT_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("dagster.daemon.controller.DAEMON_HEARTBEAT_TOLERANCE_SECONDS", 2)


def setup_instance(dagster_home):
    os.environ["DAGSTER_HOME"] = dagster_home
    config = """run_coordinator:
    module: dagster.core.run_coordinator
    class: QueuedRunCoordinator
    config:
        dequeue_interval_seconds: 1
    """
    with open(os.path.join(dagster_home, "dagster.yaml"), "w") as file:
        file.write(config)


def test_heartbeat(tmpdir, monkeypatch_daemon_heartbeat_timing):
    dagster_home_path = tmpdir.strpath
    setup_instance(dagster_home_path)

    instance = DagsterInstance.get()

    assert instance.daemon_healthy() is False

    p = subprocess.Popen(["dagster-daemon", "run"])
    time.sleep(3)

    assert instance.daemon_healthy() is True

    p.kill()

    # assert 0

    time.sleep(6)
    assert instance.daemon_healthy() is False
