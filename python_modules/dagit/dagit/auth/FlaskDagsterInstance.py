from typing import Any, Dict

from dagit.auth.DagsterAuthManager import DagsterAuthManager
from dagster import check
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.instance import DagsterInstance
from dagster.core.instance.config import config_field_for_configurable_class
from dagster.core.instance.ref import configurable_class_data


class FlaskDagsterInstance(DagsterInstance):
    def __init__(self, auth_manager: Dict[str, Any], *args, **kwargs):
        auth_manager = check.dict_param(auth_manager, "auth_manager", key_type=str)

        self._auth_manager = configurable_class_data(auth_manager).rehydrate()

        super().__init__(*args, **kwargs)

    @staticmethod
    def get() -> "FlaskDagsterInstance":  # pylint: disable=arguments-differ
        instance = DagsterInstance.get()
        if not isinstance(instance, FlaskDagsterInstance):
            raise DagsterInvariantViolationError(
                "DagsterInstance.get() did not return a FlaskDagsterInstance. Make sure that "
                "your `dagster.yaml` file is correctly configured. For test, you can use the "
                "following config:\n\n"
                "custom_instance_class:\n"
                "  module: dagster.auth\n"
                "  class: FlaskDagsterInstance\n"
                "auth_manager:\n"
                "  module: dagit.auth\n"
                "  class: DagsterHeaderAuthManger\n"
                "  config: {}"
            )
        return instance

    @classmethod
    def config_schema(cls):
        return {
            "auth_manager": config_field_for_configurable_class(),
        }

    @property
    def auth_manager(self) -> DagsterAuthManager:
        return self._auth_manager

    def get_identity(self) -> str:
        return self.auth_manager.get_identity()
