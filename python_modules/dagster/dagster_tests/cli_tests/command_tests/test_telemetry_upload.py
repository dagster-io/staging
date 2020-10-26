import logging
import os

import mock
import pytest
import responses
from click.testing import CliRunner

from dagster.cli.pipeline import pipeline_execute_command
from dagster.core.telemetry import DAGSTER_TELEMETRY_URL, upload_logs
from dagster.core.test_utils import environ, instance_for_test
from dagster.utils import pushd, script_relative_path


def path_to_file(path):
    return script_relative_path(os.path.join("./", path))


@pytest.mark.skipif(
    os.name == "nt", reason="TemporaryDirectory disabled for win because of event.log contention"
)
@responses.activate
def test_dagster_telemetry_upload(caplog):
    logging.getLogger("temp").setLevel(logging.INFO)
    logger = logging.getLogger("dagster_telemetry_logger")
    for handler in logger.handlers:
        logger.removeHandler(handler)

    with instance_for_test(enable_telemetry=True):
        runner = CliRunner(env={"DAGSTER_HOME": os.getenv("DAGSTER_HOME")})
        with pushd(path_to_file("")):
            pipeline_attribute = "foo_pipeline"
            runner.invoke(
                pipeline_execute_command,
                ["-f", path_to_file("test_cli_commands.py"), "-a", pipeline_attribute],
            )

        mock_stop_event = mock.MagicMock()
        mock_stop_event.is_set.return_value = False

        def side_effect():
            mock_stop_event.is_set.return_value = True

        mock_stop_event.wait.side_effect = side_effect
        logging.getLogger("temp").info(os.getenv("DAGSTER_HOME"))

        for record in caplog.records:
            logger.handlers[0].emit(record)

        upload_logs(mock_stop_event)
        assert responses.assert_call_count(DAGSTER_TELEMETRY_URL, 1)


@pytest.mark.skipif(
    os.name == "nt", reason="TemporaryDirectory disabled for win because of event.log contention"
)
@responses.activate
def test_dagster_telemetry_no_buildkite_upload():
    with environ({"BUILDKITE": "True"}):
        with instance_for_test(enable_telemetry=True):
            runner = CliRunner()
            with pushd(path_to_file("")):
                pipeline_attribute = "foo_pipeline"
                runner.invoke(
                    pipeline_execute_command,
                    ["-f", path_to_file("test_cli_commands.py"), "-a", pipeline_attribute],
                )

            upload_logs(mock.MagicMock())
            assert responses.assert_call_count(DAGSTER_TELEMETRY_URL, 0)
