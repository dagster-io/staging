import contextlib
import os
import subprocess


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


@contextlib.contextmanager
def start_daemon():
    p = subprocess.Popen(["dagster-daemon", "run"])
    yield
    p.kill()
