import logging
import sys

import pendulum
from dagster.utils.log import default_format_string


def _mockable_localtime(_):
    now_time = pendulum.now()
    return now_time.timetuple()


def get_default_daemon_logger(logging_level=logging.INFO):
    handler = logging.StreamHandler(sys.stdout)
    logger = logging.getLogger("dagster-daemon")
    logger.setLevel(logging_level)
    logger.handlers = [handler]

    formatter = logging.Formatter(default_format_string(), "%Y-%m-%d %H:%M:%S")

    formatter.converter = _mockable_localtime

    handler.setFormatter(formatter)
    return logger
