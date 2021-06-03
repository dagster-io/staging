import logging

from dagster import DagsterInstance, ModeDefinition, execute_pipeline, pipeline, solid
from dagster.core.execution.plan.python_logging import make_log_handler_resource


def test_logging_capture_logger_defined_outside():
    logger = logging.getLogger("some_logger")

    @solid
    def my_solid():
        logger.warning("some warning")

    @pipeline
    def my_pipeline():
        my_solid()

    instance = DagsterInstance.ephemeral()
    result = execute_pipeline(my_pipeline, instance=instance)

    event_records = instance.event_log_storage.get_logs_for_run(result.run_id)
    log_event_records = [er for er in event_records if er.user_message == "some warning"]
    assert len(log_event_records) == 1
    log_event_record = log_event_records[0]
    assert log_event_record.step_key == "my_solid"
    assert log_event_record.level == 30


def test_logging_capture_logger_defined_inside():
    @solid
    def my_solid():
        logger = logging.getLogger("some_logger")
        logger.warning("some warning")

    @pipeline
    def my_pipeline():
        my_solid()

    instance = DagsterInstance.ephemeral()
    result = execute_pipeline(my_pipeline, instance=instance)

    event_records = instance.event_log_storage.get_logs_for_run(result.run_id)
    log_event_records = [er for er in event_records if er.user_message == "some warning"]
    assert len(log_event_records) == 1
    log_event_record = log_event_records[0]
    assert log_event_record.step_key == "my_solid"
    assert log_event_record.level == 30


def test_make_log_handler_resource():
    logger = logging.getLogger("some_logger")
    mock_rollbar = []

    class MockRollbarHandler(logging.Handler):
        def emit(self, record):
            mock_rollbar.append(record.msg)

    @solid(required_resource_keys={"rollbar_log_handler"})
    def my_solid(_):
        logger.warning("some warning")

    @pipeline(
        mode_defs=[
            ModeDefinition(
                resource_defs={
                    "rollbar_log_handler": make_log_handler_resource(MockRollbarHandler())
                }
            )
        ]
    )
    def my_pipeline():
        my_solid()

    execute_pipeline(my_pipeline)
    assert mock_rollbar == ["some warning"]
