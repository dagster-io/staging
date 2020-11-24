from abc import ABC, abstractmethod

from dagster import check
from dagster.config.field import Field
from dagster.config.field_utils import convert_potential_field
from dagster.config.validate import process_config
from dagster.core.errors import DagsterConfigMappingFunctionError, user_code_error_boundary


def convert_user_facing_definition_config_schema(potential_schema):
    return (
        None
        if potential_schema is None
        else potential_schema
        if isinstance(potential_schema, IDefinitionConfigSchema)
        else DefinitionConfigSchema(convert_potential_field(potential_schema))
    )


class IDefinitionConfigSchema(ABC):
    @abstractmethod
    def as_field(self):
        raise NotImplementedError()

    @property
    def config_type(self):
        field = self.as_field()
        return field.config_type if field else None


class DefinitionConfigSchema(IDefinitionConfigSchema):
    def __init__(self, config_field):
        self._config_field = check.inst_param(config_field, "config_field", Field)

    def as_field(self):
        return self._config_field


class ConfiguredDefinitionConfigSchema(IDefinitionConfigSchema):
    def __init__(self, current_field, config_mapping_fn):
        self._current_field = check.opt_inst_param(current_field, "current_field", Field)
        self.config_mapping_fn = check.callable_param(config_mapping_fn, "config_mapping_fn")

    def as_field(self):
        return self._current_field

    @staticmethod
    def for_configured_definition(configured_definition, config_schema, config_or_config_fn):
        return ConfiguredDefinitionConfigSchema(
            config_schema.as_field() if config_schema else None,
            _get_wrapped_config_mapping_fn(
                configured_definition, config_or_config_fn, config_schema
            ),
        )


def _get_user_code_error_str_lambda(configured_definition):
    return lambda: (
        "The config mapping function on a `configured` {} has thrown an unexpected "
        "error during its execution."
    ).format(configured_definition.__class__.__name__)


def _get_wrapped_config_mapping_fn(configured_definition, config_or_config_fn, config_schema):
    """
    Encapsulates the recursiveness of the `configurable`
    pattern by returning a closure that invokes `self.apply_config_mapping` (belonging to this
    parent object) on the mapping function or static config passed into this method.
    """

    def _to_config_fn():
        # Provide either a config mapping function with noneable schema...
        if callable(config_or_config_fn):
            return config_or_config_fn
        else:  # or a static config object (and no schema).
            check.invariant(
                config_schema is None,
                "When non-callable config is given, config_schema must be None",
            )
            # Stub a config mapping function from the static config object.
            return lambda _: config_or_config_fn

    config_fn = _to_config_fn()

    def wrapped_config_mapping_fn(validated_and_resolved_config):
        """
        Given validated and resolved configuration for this ConfigurableMixin, applies the
        provided config mapping function, validates its output against the inner resource's
        config_schema, and recursively invoked the `apply_config_mapping` method on the resource
        """
        check.dict_param(validated_and_resolved_config, "validated_and_resolved_config")
        with user_code_error_boundary(
            DagsterConfigMappingFunctionError,
            _get_user_code_error_str_lambda(configured_definition),
        ):
            mapped_config = {"config": config_fn(validated_and_resolved_config.get("config", {}))}
        # Validate mapped_config against the inner resource's config_schema (on self).
        config_evr = process_config(
            {"config": configured_definition.config_field or {}}, mapped_config
        )
        if config_evr.success:
            return configured_definition.apply_config_mapping(config_evr.value)  # Recursive step
        else:
            return config_evr  # Bubble up the errors

    return wrapped_config_mapping_fn
