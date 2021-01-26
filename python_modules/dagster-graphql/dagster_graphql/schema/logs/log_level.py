import logging

import graphene
from dagster import check


class LogLevel(graphene.Enum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    INFO = "INFO"
    WARNING = "WARNING"
    DEBUG = "DEBUG"

    @classmethod
    def from_level(cls, level):
        check.int_param(level, "level")
        if level == logging.CRITICAL:
            return LogLevel.CRITICAL
        elif level == logging.ERROR:
            return LogLevel.ERROR
        elif level == logging.INFO:
            return LogLevel.INFO
        elif level == logging.WARNING:
            return LogLevel.WARNING
        elif level == logging.DEBUG:
            return LogLevel.DEBUG
        else:
            check.failed("Invalid log level: {level}".format(level=level))
