import logging
from dagster.core.instance.init_logging import initialize_logging
import multiprocessing
import tempfile
from pathlib import Path
from typing import Dict, Any

FILE_CONFIG_TEMPLATE = """
[loggers]
keys=root

[handlers]
keys=file_handler

[formatters]
keys=

[logger_root]
handlers=file_handler
level=DEBUG

[handler_file_handler]
class=logging.FileHandler
args=["{logfile_path}"]
"""


def get_dict_config(logfile_path: str) -> Dict[str, Any]:
    return {
        "config": {
            "root": {"handlers": ["file_handler"], "level": "DEBUG"},
            "handlers": {
                "file_handler": {
                    "class": "logging.FileHandler",
                    "filename": logfile_path,
                }
            },
        }
    }


def test_initialize_logging_file_config():
    with tempfile.TemporaryDirectory() as logdir:
        logfile_path = str(Path(logdir) / "logfile")

        file_config = FILE_CONFIG_TEMPLATE.format(logfile_path=logfile_path)

        config_file_path = str(Path(logdir) / "logging_config.ini")
        with open(config_file_path, "w") as f:
            f.write(file_config)

        # run in a subprocess so it doesn't pollute the test process's global logging config
        def subprocess():
            initialize_logging({"file": config_file_path})
            logging.debug("hello")

        p = multiprocessing.Process(target=subprocess)
        p.start()
        p.join()

        logfile_contents = open(logfile_path, "r").read()
        assert len(logfile_contents) > 0
        assert "hello" in logfile_contents


def test_initialize_logging_dict_config():
    with tempfile.TemporaryDirectory() as logdir:
        logfile_path = str(Path(logdir) / "logfile")

        # run in a subprocess so it doesn't pollute the test process's global logging config
        def subprocess():
            initialize_logging(get_dict_config(logfile_path))
            logging.debug("hello")

        p = multiprocessing.Process(target=subprocess)
        p.start()
        p.join()

        logfile_contents = open(logfile_path, "r").read()
        assert len(logfile_contents) > 0
        assert "hello" in logfile_contents
