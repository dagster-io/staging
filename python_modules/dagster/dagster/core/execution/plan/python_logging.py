import logging
from contextlib import contextmanager

from dagster.core.log_manager import DagsterLogManager


class DagsterLoggingHandler(logging.Handler):
    def __init__(self, log_manager: DagsterLogManager):
        self.log_manager = log_manager
        super(DagsterLoggingHandler, self).__init__()

    def emit(self, record):
        self.log_manager.log(level=record.levelno, msg=record.msg)


@contextmanager
def python_logging_to_dagster_log_manager(log_manager: DagsterLogManager):
    """Routes all Pythong logging logs to the given DagsterLogManager"""
    handler = DagsterLoggingHandler(log_manager)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    try:
        yield
    finally:
        root_logger.removeHandler(handler)
