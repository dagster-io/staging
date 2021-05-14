from contextlib import contextmanager

from dagster import check
from dagster.serdes import ConfigurableClass, ConfigurableClassData

from .compute_log_manager import ComputeLogData, ComputeLogManager


class NoOpComputeLogManager(ComputeLogManager, ConfigurableClass):
    def __init__(self, inst_data=None):
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @staticmethod
    def from_config_value(inst_data, config_value):
        return NoOpComputeLogManager(inst_data=inst_data, **config_value)

    @contextmanager
    def capture_logs(self, namespace: str, log_key: str):
        pass

    def get_logs(
        self, namespace: str, log_key: str, cursor: str = None, max_file_bytes: str = None
    ) -> ComputeLogData:
        return ComputeLogData()

    def is_capture_complete(self, namespace: str, log_key: str):
        return True

    def use_legacy_api(self) -> bool:
        return True
