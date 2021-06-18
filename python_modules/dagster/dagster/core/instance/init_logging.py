from logging.config import dictConfig, fileConfig
from typing import Any, Dict, Optional

from dagster import check
from dagster.utils.merger import merge_dicts


def initialize_logging(logging_config: Optional[Dict[str, Any]]) -> None:
    if logging_config:
        if "file" in logging_config:
            fileConfig(logging_config["file"])
        elif "config" in logging_config:
            dict_config = merge_dicts({"version": 1}, logging_config["config"])
            dictConfig(dict_config)
        elif len(logging_config) > 0:
            check.failed(f"Unexpected logging config keys {logging_config.keys()}")
