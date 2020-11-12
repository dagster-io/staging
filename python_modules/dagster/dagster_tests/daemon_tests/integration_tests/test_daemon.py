import os
import subprocess
import time

from dagster import DagsterInstance


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


def test_heartbeat(tmpdir):
    dagster_home_path = tmpdir.strpath
    setup_instance(dagster_home_path)

    instance = DagsterInstance.get()

    assert instance._event_storage.daemon_healthy() is False

    p = subprocess.Popen(["dagster-daemon", "run"])
    time.sleep(10)

    assert instance._event_storage.daemon_healthy() is True

    p.kill()

    # assert 0

    time.sleep(120)
    assert instance._event_storage.daemon_healthy() is False
