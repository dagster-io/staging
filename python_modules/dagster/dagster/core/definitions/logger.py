import logging
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from dagster import check
from dagster.core.definitions.config import is_callable_valid_config_arg
from dagster.core.definitions.configurable import AnonymousConfigurableDefinition
from dagster.core.errors import DagsterInvalidConfigError

from .definition_config_schema import convert_user_facing_definition_config_schema

if TYPE_CHECKING:
    from dagster.core.execution.context.logger import InitLoggerContext, UnboundInitLoggerContext
    from dagster.core.definitions import PipelineDefinition

    InitLoggerFunction = Callable[[InitLoggerContext], logging.Logger]


class LoggerDefinition(AnonymousConfigurableDefinition):
    """Core class for defining loggers.

    Loggers are pipeline-scoped logging handlers, which will be automatically invoked whenever
    solids in a pipeline log messages.

    Args:
        logger_fn (Callable[[InitLoggerContext], logging.Logger]): User-provided function to
            instantiate the logger. This logger will be automatically invoked whenever the methods
            on ``context.log`` are called from within solid compute logic.
        config_schema (Optional[ConfigSchema]): The schema for the config. Configuration data available in
            `init_context.logger_config`. If not set, Dagster will accept any config provided.
        description (Optional[str]): A human-readable description of this logger.
    """

    def __init__(
        self,
        logger_fn: "InitLoggerFunction",
        config_schema: Any = None,
        description: Optional[str] = None,
    ):
        self._logger_fn = check.callable_param(logger_fn, "logger_fn")
        self._config_schema = convert_user_facing_definition_config_schema(config_schema)
        self._description = check.opt_str_param(description, "description")

    # This allows us to pass LoggerDefinition off as a function, so that we can use it as a bare
    # decorator
    def __call__(self, *args, **kwargs):
        return self

    @property
    def logger_fn(self) -> "InitLoggerFunction":
        return self._logger_fn

    @property
    def config_schema(self) -> Any:
        return self._config_schema

    @property
    def description(self) -> Optional[str]:
        return self._description

    def copy_for_configured(
        self, description: Optional[str], config_schema: Any, _
    ) -> "LoggerDefinition":
        return LoggerDefinition(
            config_schema=config_schema,
            description=description or self.description,
            logger_fn=self.logger_fn,
        )

    def initialize(self, init_context: "UnboundInitLoggerContext") -> logging.Logger:
        """Using the provided context, call the underlying `logger_fn` and return created logger."""
        from dagster.core.execution.context.logger import (
            InitLoggerContext,
            UnboundInitLoggerContext,
        )

        check.inst_param(init_context, "init_context", UnboundInitLoggerContext)

        logger_config = _resolve_bound_config(init_context.logger_config, self)

        bound_context = InitLoggerContext(
            logger_config, self, init_context.pipeline_def, init_context.run_id
        )

        return self.logger_fn(bound_context)


def _resolve_bound_config(logger_config: Any, logger_def: "LoggerDefinition") -> Any:
    from dagster.config.validate import process_config

    validated_config = None
    if logger_def.config_field:
        logger_config = logger_config or _get_default_if_exists(logger_def)
        config_evr = process_config(logger_def.config_field.config_type, logger_config)
        if not config_evr.success:
            raise DagsterInvalidConfigError(
                "Error in config for logger ",
                config_evr.errors,
                logger_config,
            )
        validated_config = config_evr.value
        mapped_config_evr = logger_def.apply_config_mapping({"config": validated_config})
        if not mapped_config_evr.success:
            raise DagsterInvalidConfigError(
                "Error in config mapping for logger ", mapped_config_evr.errors, validated_config
            )
        validated_config = mapped_config_evr.value.get("config", {})
    return validated_config or {}


def _get_default_if_exists(logger_def: LoggerDefinition):
    return (
        logger_def.config_field.default_value
        if logger_def.config_field and logger_def.config_field.default_provided
        else None
    )


def logger(
    config_schema: Any = None, description: Optional[str] = None
) -> Union["LoggerDefinition", Callable[["InitLoggerFunction"], "LoggerDefinition"]]:
    """Define a logger.

    The decorated function should accept an :py:class:`InitLoggerContext` and return an instance of
    :py:class:`python:logging.Logger`. This function will become the ``logger_fn`` of an underlying
    :py:class:`LoggerDefinition`.

    Args:
        config_schema (Optional[ConfigSchema]): The schema for the config. Configuration data available in
            `init_context.logger_config`. If not set, Dagster will accept any config provided.
        description (Optional[str]): A human-readable description of the logger.
    """
    # This case is for when decorator is used bare, without arguments.
    # E.g. @logger versus @logger()
    if callable(config_schema) and not is_callable_valid_config_arg(config_schema):
        return LoggerDefinition(logger_fn=config_schema)

    def _wrap(logger_fn: "InitLoggerFunction") -> "LoggerDefinition":
        return LoggerDefinition(
            logger_fn=logger_fn,
            config_schema=config_schema,
            description=description,
        )

    return _wrap


def build_init_logger_context(
    logger_config: Any = None,
    pipeline_def: Optional["PipelineDefinition"] = None,
) -> "UnboundInitLoggerContext":
    from dagster.core.execution.context.logger import UnboundInitLoggerContext
    from dagster.core.definitions import PipelineDefinition

    check.opt_inst_param(pipeline_def, "pipeline_def", PipelineDefinition)

    return UnboundInitLoggerContext(logger_config=logger_config, pipeline_def=pipeline_def)
