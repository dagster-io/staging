from abc import ABC, abstractmethod, abstractproperty
from functools import update_wrapper

from dagster import check
from dagster.config.field_utils import check_user_facing_opt_config_param
from dagster.core.definitions.config import is_callable_valid_config_arg
from dagster.core.definitions.resource import ResourceDefinition


class HasOutputConfigSchema(ABC):
    @abstractproperty
    def output_config_schema(self):
        pass


class OutputManagerDefinition(ResourceDefinition, HasOutputConfigSchema):
    def __init__(
        self,
        resource_fn=None,
        config_schema=None,
        description=None,
        _configured_config_mapping_fn=None,
        version=None,
        output_config_schema=None,
    ):
        self._output_config_schema = output_config_schema
        super(OutputManagerDefinition, self).__init__(
            resource_fn=resource_fn,
            config_schema=config_schema,
            description=description,
            _configured_config_mapping_fn=_configured_config_mapping_fn,
            version=version,
        )

    @property
    def output_config_schema(self):
        return self._output_config_schema


class OutputManager(ABC):
    @abstractmethod
    def set_asset(self, context, obj):
        """The user-definied write method that stores a data object.

        Args:
            context (AssetStoreContext): The context of the step output that produces this asset.
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


class NoInitOutputManager(OutputManager):
    def __init__(self, config, process_fn):
        self._config = config
        self._process_fn = process_fn

    def set_asset(self, context, obj):
        return self._process_fn(context, self._config, context.input_config, obj)


class _OutputManagerDecoratorCallable:
    def __init__(
        self, config_schema=None, description=None, version=None, output_config_schema=None,
    ):
        self.config_schema = check_user_facing_opt_config_param(config_schema, "config_schema")
        self.description = check.opt_str_param(description, "description")
        self.version = check.opt_str_param(version, "version")
        self.output_config_schema = check_user_facing_opt_config_param(
            output_config_schema, "output_config_schema"
        )

    def __call__(self, load_fn):
        check.callable_param(load_fn, "load_fn")

        def _resource_fn(init_context):
            return NoInitOutputManager(init_context.resource_config, load_fn)

        output_manager_def = OutputManagerDefinition(
            resource_fn=_resource_fn,
            config_schema=self.config_schema,
            description=self.description,
            version=self.version,
            output_config_schema=self.output_config_schema,
        )

        update_wrapper(output_manager_def, wrapped=load_fn)

        return output_manager_def
