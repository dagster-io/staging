from abc import ABC, abstractmethod
from functools import update_wrapper

from dagster import check
from dagster.config.field_utils import check_user_facing_opt_config_param
from dagster.core.definitions.config import is_callable_valid_config_arg
from dagster.core.definitions.resource import ResourceDefinition


class InputManagerDefinition(ResourceDefinition):
    def __init__(
        self,
        resource_fn=None,
        config_schema=None,
        description=None,
        _configured_config_mapping_fn=None,
        version=None,
        input_config_schema=None,
    ):
        self._input_config_schema = input_config_schema
        super(InputManagerDefinition, self).__init__(
            resource_fn=resource_fn,
            config_schema=config_schema,
            description=description,
            _configured_config_mapping_fn=_configured_config_mapping_fn,
            version=version,
        )

    @property
    def input_config_schema(self):
        return self._input_config_schema


class InputManager(ABC):
    @abstractmethod
    def get_asset(self, context):
        """The user-defined read method that loads data given its metadata.

        Args:
            context (AssetStoreContext): The context of the step output that produces this asset.

        Returns:
            Any: The data object.
        """


def input_manager(config_schema=None, description=None, input_config_schema=None, version=None):
    if callable(config_schema) and not is_callable_valid_config_arg(config_schema):
        return _InputManagerDecoratorCallable()(config_schema)

    def _wrap(load_fn):
        return _InputManagerDecoratorCallable(
            config_schema=config_schema,
            description=description,
            version=version,
            input_config_schema=input_config_schema,
        )(load_fn)

    return _wrap


class NoInitInputManager(InputManager):
    def __init__(self, config, load_fn):
        self._config = config
        self._load_fn = load_fn

    def get_asset(self, context):
        return self._load_fn(context, self._config, context.input_config)


class _InputManagerDecoratorCallable:
    def __init__(
        self, config_schema=None, description=None, version=None, input_config_schema=None,
    ):
        self.config_schema = check_user_facing_opt_config_param(config_schema, "config_schema")
        self.description = check.opt_str_param(description, "description")
        self.version = check.opt_str_param(version, "version")
        self.input_config_schema = check_user_facing_opt_config_param(
            input_config_schema, "input_config_schema"
        )

    def __call__(self, load_fn):
        check.callable_param(load_fn, "load_fn")

        def _resource_fn(init_context):
            return NoInitInputManager(init_context.resource_config, load_fn)

        input_manager_def = InputManagerDefinition(
            resource_fn=_resource_fn,
            config_schema=self.config_schema,
            description=self.description,
            version=self.version,
            input_config_schema=self.input_config_schema,
        )

        update_wrapper(input_manager_def, wrapped=load_fn)

        return input_manager_def
