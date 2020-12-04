from abc import ABC, abstractmethod, abstractproperty
from functools import update_wrapper

from dagster import check
from dagster.core.definitions.config import is_callable_valid_config_arg
from dagster.core.definitions.definition_config_schema import (
    convert_user_facing_definition_config_schema,
)
from dagster.core.definitions.resource import ResourceDefinition


class IOutputManagerDefinition:
    @abstractproperty
    def output_config_schema(self):
        """The schema for per-output configuration for outputs that are managed by this
        input manager"""


class OutputManagerDefinition(ResourceDefinition, IOutputManagerDefinition):
    def __init__(
        self,
        resource_fn=None,
        config_schema=None,
        description=None,
        version=None,
        output_config_schema=None,
    ):
        self._output_config_schema = convert_user_facing_definition_config_schema(
            output_config_schema
        )
        super(OutputManagerDefinition, self).__init__(
            resource_fn=resource_fn,
            config_schema=config_schema,
            description=description,
            version=version,
        )

    @property
    def output_config_schema(self):
        return self._output_config_schema


class OutputManager(ABC):
    @abstractmethod
    def materialize(self, context, obj):
        """The user-definied write method that stores a data object.

        Args:
            context (OutputContext): The context of the step output that produces this asset.
            obj (Any): The data object to be stored.
        """


def output_manager(config_schema=None, description=None, output_config_schema=None, version=None):
    if callable(config_schema) and not is_callable_valid_config_arg(config_schema):
        return _OutputManagerDecoratorCallable()(config_schema)

    def _wrap(load_fn):
        return _OutputManagerDecoratorCallable(
            config_schema=config_schema,
            description=description,
            version=version,
            output_config_schema=output_config_schema,
        )(load_fn)

    return _wrap


class ResourceConfigPassthroughInputManager(OutputManager):
    def __init__(self, config, process_fn):
        self._config = config
        self._process_fn = process_fn

    def materialize(self, context, obj):
        return self._process_fn(context, self._config, obj)


class _OutputManagerDecoratorCallable:
    def __init__(
        self, config_schema=None, description=None, version=None, output_config_schema=None,
    ):
        #  type checks in definition
        self.config_schema = config_schema
        self.description = description
        self.version = version
        self.output_config_schema = output_config_schema

    def __call__(self, load_fn):
        check.callable_param(load_fn, "load_fn")

        def _resource_fn(init_context):
            return ResourceConfigPassthroughInputManager(init_context.resource_config, load_fn)

        output_manager_def = OutputManagerDefinition(
            resource_fn=_resource_fn,
            config_schema=self.config_schema,
            description=self.description,
            version=self.version,
            output_config_schema=self.output_config_schema,
        )

        update_wrapper(output_manager_def, wrapped=load_fn)

        return output_manager_def
