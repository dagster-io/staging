from abc import ABC, abstractmethod, abstractproperty
from functools import update_wrapper

from dagster import Field, check
from dagster.config.field_utils import Selector, check_user_facing_opt_config_param
from dagster.core.definitions.config import is_callable_valid_config_arg
from dagster.core.definitions.resource import ResourceDefinition
from dagster.core.errors import DagsterInvariantViolationError


class IInputManagerDefinition(ABC):
    @abstractproperty
    def input_config_schema(self):
        pass


class InputManagerDefinition(ResourceDefinition, IInputManagerDefinition):
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


class CompositeInputManager:
    def __init__(self, input_managers, default=None):
        self._input_managers = input_managers
        self._default = default

    def get_asset(self, context):
        if context.input_config:
            input_manager_key = list(context.input_config.keys())[0]
        elif self._default:
            input_manager_key = self._default
        else:
            raise DagsterInvariantViolationError("TODO MESSAGE")

        chosen_input_manager = self._input_managers[input_manager_key]
        sub_context = context.replace_input_config(
            (context.input_config or {}).get(input_manager_key)
        )
        return chosen_input_manager.get_asset(sub_context)


def build_composite_input_manager(input_manager_defs, default=None):
    input_config_schema = Field(
        Selector(
            {
                key: manager_def.input_config_schema
                for key, manager_def in input_manager_defs.items()
                if manager_def.input_config_schema
            }
        ),
        is_required=(default is None),
    )
    resource_config_schema = {
        key: manager_def.config_schema
        for key, manager_def in input_manager_defs.items()
        if manager_def.config_schema
    }

    def resource_fn(init_context):
        config = init_context.resource_config
        input_managers = {
            key: manager_def.resource_fn(init_context.replace_config(config.get(key)))
            for key, manager_def in input_manager_defs.items()
        }
        return CompositeInputManager(input_managers, default=default)

    return InputManagerDefinition(
        resource_fn=resource_fn,
        config_schema=resource_config_schema,
        input_config_schema=input_config_schema,
    )
