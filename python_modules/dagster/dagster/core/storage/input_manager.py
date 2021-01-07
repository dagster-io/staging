from abc import ABC, abstractmethod
from functools import update_wrapper

from dagster import check
from dagster.core.definitions.config import is_callable_valid_config_arg
from dagster.core.definitions.definition_config_schema import (
    convert_user_facing_definition_config_schema,
)
from dagster.core.definitions.resource import ResourceDefinition
from dagster.core.errors import DagsterInvalidDefinitionError
from dagster.core.types.dagster_type import DagsterTypeKind


class IInputManagerDefinition:
    @abstractmethod
    def get_input_config_schema(self, input_def):
        """The schema for per-input configuration for inputs that are managed by this
        input manager"""


class InputManagerDefinition(ResourceDefinition, IInputManagerDefinition):
    """Definition of an input manager resource.

    An InputManagerDefinition is a :py:class:`ResourceDefinition` whose resource_fn returns an
    :py:class:`InputManager`.  InputManagers are used to load the inputs to solids.
    """

    def __init__(
        self,
        resource_fn=None,
        config_schema=None,
        description=None,
        input_config_schema=None,
        input_config_schema_fn=None,
        required_resource_keys=None,
        version=None,
    ):
        if input_config_schema_fn:
            self._input_config_schema_fn = input_config_schema_fn
        else:
            self._input_config_schema_fn = lambda _context: input_config_schema

        super(InputManagerDefinition, self).__init__(
            resource_fn=resource_fn,
            config_schema=config_schema,
            description=description,
            required_resource_keys=required_resource_keys,
            version=version,
        )

    def get_input_config_schema(self, input_def):
        definition_config_schema = convert_user_facing_definition_config_schema(
            self._input_config_schema_fn(input_def)
        )

        return definition_config_schema.config_type if definition_config_schema else None

    def copy_for_configured(self, name, description, config_schema, _):
        check.invariant(name is None, "ResourceDefintions do not have names")
        return InputManagerDefinition(
            config_schema=config_schema,
            description=description or self.description,
            resource_fn=self.resource_fn,
            required_resource_keys=self.required_resource_keys,
            input_config_schema_fn=self._input_config_schema_fn,
        )


class InputManager(ABC):
    """InputManagers are used to load the inputs to solids.

    The easiest way to define an InputManager is with the :py:function:`input_manager` decorator.
    """

    @abstractmethod
    def load_input(self, context):
        """The user-defined read method that loads data given its metadata.

        Args:
            context (InputContext): The context of the step output that produces this asset.

        Returns:
            Any: The data object.
        """


def input_manager(
    config_schema=None,
    description=None,
    input_config_schema=None,
    input_config_schema_fn=None,
    required_resource_keys=None,
    version=None,
):
    """Define an input manager.

    The decorated function should accept a :py:class:`InputContext` and resource config, and return
    a loaded object that will be passed into one of the inputs of a solid.

    The decorator produces an :py:class:`InputManagerDefinition`.

    Args:
        config_schema (Optional[ConfigSchema]): The schema for the resource-level config.
        description (Optional[str]): A human-readable description of the resource.
        input_config_schema (Optional[ConfigSchema]): A schema for the input-level config. Each
            input that uses this input manager can be configured separately using this config.
        required_resource_keys (Optional[Set[str]]): Keys for the resources required by the input
            manager.
        version (Optional[str]): (Experimental) the version of the input manager definition.

    **Examples:**

    .. code-block:: python

        @input_manager
        def csv_loader(_):
            return read_csv("some/path")

        @solid(input_defs=[InputDefinition("input1", manager_key="csv_loader_key")])
        def my_solid(_, input1):
            do_stuff(input1)

        @pipeline(mode_defs=[ModeDefinition(resource_defs={"csv_loader_key": csv_loader})])
        def my_pipeline():
            my_solid()

        @input_manager(config_schema={"base_dir": str})
        def csv_loader(context):
            return read_csv(context.resource_config["base_dir"] + "/some/path")

        @input_manager(input_config_schema={"path": str})
        def csv_loader(context):
            return read_csv(context.config["path"])
    """

    if callable(config_schema) and not is_callable_valid_config_arg(config_schema):
        return _InputManagerDecoratorCallable()(config_schema)

    def _wrap(load_fn):
        return _InputManagerDecoratorCallable(
            config_schema=config_schema,
            description=description,
            version=version,
            input_config_schema=input_config_schema,
            input_config_schema_fn=input_config_schema_fn,
            required_resource_keys=required_resource_keys,
        )(load_fn)

    return _wrap


class InputManagerWrapper(InputManager):
    def __init__(self, load_fn):
        self._load_fn = load_fn

    def load_input(self, context):
        return self._load_fn(context)


class _InputManagerDecoratorCallable:
    def __init__(
        self,
        config_schema=None,
        description=None,
        version=None,
        input_config_schema=None,
        input_config_schema_fn=None,
        required_resource_keys=None,
    ):
        self.config_schema = config_schema
        self.description = check.opt_str_param(description, "description")
        self.version = check.opt_str_param(version, "version")
        self.input_config_schema = input_config_schema
        self.input_config_schema_fn = input_config_schema_fn
        self.required_resource_keys = required_resource_keys

    def __call__(self, load_fn):
        check.callable_param(load_fn, "load_fn")

        def _resource_fn(_):
            return InputManagerWrapper(load_fn)

        input_manager_def = InputManagerDefinition(
            resource_fn=_resource_fn,
            config_schema=self.config_schema,
            description=self.description,
            version=self.version,
            input_config_schema=self.input_config_schema,
            input_config_schema_fn=self.input_config_schema_fn,
            required_resource_keys=self.required_resource_keys,
        )

        update_wrapper(input_manager_def, wrapped=load_fn)

        return input_manager_def


def type_based_root_input_manager(type_loaders):
    """
    Returns a root input manager definition that loads inputs based on the dagster type of the input
    definition being loaded.

    Includes a set of built-in loaders for primitive dagster types like Int, String, etc.

    Args:
        type_loaders (List[Tuple(DagsterType, Loader)]): Each entry is a dagster type and the loader
            that will be used to load inputs with that type.
    """

    from dagster.core.types.dagster_type import resolve_dagster_type

    # TODO: augment the dict with loaders for builtin types
    loaders_by_type_name = {
        resolve_dagster_type(dagster_type).key: loader for dagster_type, loader in type_loaders
    }

    def config_schema_fn(input_def):
        if input_def.dagster_type.kind == DagsterTypeKind.NOTHING:
            return None

        type_loader = loaders_by_type_name.get(input_def.dagster_type.key)

        # This will be deprecated and eventually removed with the deprecation and removal
        # of dagster type loaders
        if type_loader is None:
            type_loader = input_def.dagster_type.loader

        if not type_loader:
            raise DagsterInvalidDefinitionError(
                f'Input "{input_def.name}" is not connected to the output of a previous solid, and '
                "the root input manager does not know how to load inputs with its dagster type "
                "from configuration. Possible solutions are:\n"
                "  * Use type_based_root_input_manager to define a root input manager with an "
                f'entry for the type "{input_def.dagster_type.display_name}"\n'
                f'  * Connect "{input_def.name}" to the output of another solid\n'
            )

        return type_loader.schema_type

    # Required resource keys is the union of required resource keys of all loaders. Maybe
    # find a way to decide required resource keys per input, based on the loader for that input.
    required_resource_keys = set().union(
        *[loader.required_resource_keys() for _, loader in type_loaders]
    )

    @input_manager(
        input_config_schema_fn=config_schema_fn, required_resource_keys=required_resource_keys
    )
    def _input_manager(context):
        type_loader = loaders_by_type_name.get(context.dagster_type.key)

        # This will be deprecated and eventually removed with the deprecation and removal
        # of dagster type loaders
        if type_loader is None:
            type_loader = context.dagster_type.loader

        return type_loader.construct_from_config_value(context, context.config)

    return _input_manager


default_input_manager = type_based_root_input_manager([])
