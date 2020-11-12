import time

import pytest

from .utils import setup_instance, start_daemon


@pytest.fixture
def monkeypatch_daemon_heartbeat_timing(monkeypatch):
    monkeypatch.setattr("dagster.daemon.controller.DAEMON_HEARTBEAT_INTERVAL_SECONDS", 0)
    monkeypatch.setattr("dagster.daemon.controller.DAEMON_HEARTBEAT_TOLERANCE_SECONDS", 2)


def test_heartbeat(
    tmpdir,
    monkeypatch_daemon_heartbeat_timing,  # pylint: disable=unused-argument, redefined-outer-name
):

    dagster_home_path = tmpdir.strpath
    with setup_instance(
        dagster_home_path,
        """run_coordinator:
    module: dagster.core.run_coordinator
    class: QueuedRunCoordinator
    """,
    ) as instance:

        assert instance.daemon_healthy() is False

        with start_daemon():
            time.sleep(3)
            assert instance.daemon_healthy() is True

        time.sleep(6)
        assert instance.daemon_healthy() is False
