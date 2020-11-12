import os
import subprocess

from dagster import DagsterInstance


def setup_instance(dagster_home):
    os.environ["DAGSTER_HOME"] = dagster_home


def test_heartbeat(tmpdir):
    dagster_home_path = tmpdir.strpath
    setup_instance(dagster_home_path)

    instance = DagsterInstance.get()

    assert not instance.run_storage.daemon_healthy()

    p = subprocess.Popen(["dagster-daemon", "run"])
    p.kill()
