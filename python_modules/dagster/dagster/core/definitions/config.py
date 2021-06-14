from typing import Any, Callable, Dict, NamedTuple, Union, Optional

from dagster import check
from dagster.builtins import BuiltinEnum
from dagster.config import ConfigType, Field, Shape
from .definition_config_schema import IDefinitionConfigSchema
from dagster.primitive_mapping import is_supported_config_python_builtin

from .definition_config_schema import convert_user_facing_definition_config_schema


def is_callable_valid_config_arg(config: Union[Callable[..., Any], Dict[str, Any]]) -> bool:
    return BuiltinEnum.contains(config) or is_supported_config_python_builtin(config)


class ConfigMapping(
    NamedTuple(
        "_ConfigMapping",
        [
            ("config_fn", Callable[[Any], Any]),
            ("config_schema", IDefinitionConfigSchema),
        ],
    )
):
    """Defines a config mapping for a composite solid.

    By specifying a config mapping function, you can override the configuration for the child
    solids contained within a composite solid.

    Config mappings require the configuration schema to be specified as ``config_schema``, which will
    be exposed as the configuration schema for the composite solid, as well as a configuration mapping
    function, ``config_fn``, which maps the config provided to the composite solid to the config
    that will be provided to the child solids.

    Args:
        config_fn (Callable[[dict], dict]): The function that will be called
            to map the composite config to a config appropriate for the child solids.
        config_schema (ConfigSchema): The schema of the composite config.
    """

    def __new__(
        cls,
        config_fn: Callable[[Any], Any],
        config_schema: Any = None,
    ):
        return super(ConfigMapping, cls).__new__(
            cls,
            config_fn=check.callable_param(config_fn, "config_fn"),
            config_schema=convert_user_facing_definition_config_schema(config_schema),
        )


class ConfigChanges(NamedTuple):
    default_config: Any
    config_mapping: Optional[ConfigMapping]

    def apply_over(self, inner_type: ConfigType):
        target_type = inner_type

        if self.config_mapping:
            mapped_type = self.config_mapping.config_schema.config_type
            if not isinstance(mapped_type, Shape):
                check.failed("config_mapping type must be Shape")
            target_type = mapped_type

        if self.default_config:
            if not isinstance(target_type, Shape):
                check.failed("target type type must be Shape")

            print("set ", self.default_config)
            target_type = _set_default(target_type, self.default_config)

        return target_type


def _set_default(config_shape: Shape, cfg_value: Dict[str, Any]) -> Shape:
    """Change a config schema to set a default value"""
    updated_fields = {}
    for name, field in config_shape.fields.items():
        if name in cfg_value:
            updated_fields[name] = Field(
                config=field.config_type,
                default_value=cfg_value[name],
                description=field.description,
            )
        else:
            updated_fields[name] = field

    return Shape(
        fields=updated_fields,
        description="run config schema with default values from default_config",
    )
