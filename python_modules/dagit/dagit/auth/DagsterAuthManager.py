from abc import ABC, abstractmethod

from dagster.serdes.config_class import ConfigurableClass
from flask import request


class DagsterAuthManager(ABC):
    @abstractmethod
    def get_identity(self) -> str:
        """Return the username of the current user."""


class DagsterHardcodedAuthManager(DagsterAuthManager, ConfigurableClass):
    @property
    def inst_data(self):
        return None

    @classmethod
    def config_type(cls):
        return None

    @staticmethod
    def from_config_value(_inst_data, _config_value):
        return DagsterHardcodedAuthManager()

    def get_identity(self):
        return "Foo"


class DagsterHeaderAuthManager(DagsterAuthManager, ConfigurableClass):
    @property
    def inst_data(self):
        return None

    @classmethod
    def config_type(cls):
        return None

    @staticmethod
    def from_config_value(_inst_data, _config_value):
        return DagsterHeaderAuthManager()

    def get_identity(self):
        try:
            # https://tedboy.github.io/flask/flask_doc.reqcontext.html#notes-on-proxies
            request._get_current_object()  # pylint: disable=protected-access
        except RuntimeError:
            raise Exception("Flask request not found")

        username = request.headers.get("X-Dagster-User", None)
        if not username:
            raise Exception("Header not found")

        return username
