from functools import update_wrapper

from dagster import check
from dagster.builtins import Int
from dagster.config.field import Field
from dagster.core.execution.retries import Retries, get_retries_config

from .definition_config_schema import convert_user_facing_definition_config_schema
from .resource import IResourceDefinition


class ExecutorDefinition(IResourceDefinition):
    """
    Args:
        name (Optional[str]): The name of the executor.
        config_schema (Optional[ConfigSchema]): The schema for the config. Configuration data
            available in `init_context.executor_config`.
        resource_fn (Callable): Should accept an :py:class:`InitExecutorContext`
            and return an instance of :py:class:`Executor`
        required_resource_keys (Optional[Set[str]]): Keys for the resources required by the
            executor.
    """

    def __init__(
        self,
        name,
        resource_fn=None,
        config_schema=None,
        description=None,
        requires_multiprocess_safe_env=False,
    ):
        self._name = check.str_param(name, "name")
        self._resource_fn = check.callable_param(resource_fn, "resource_fn")
        self._config_schema = convert_user_facing_definition_config_schema(config_schema)
        self._description = check.opt_str_param(description, "description")
        self._requires_multiprocess_safe_env = requires_multiprocess_safe_env

    @property
    def name(self):
        return self._name

    @property
    def description(self):
        return self._description

    @property
    def config_schema(self):
        return self._config_schema

    @property
    def resource_fn(self):
        return self._resource_fn

    @property
    def requires_multiprocess_safe_env(self):
        return self._requires_multiprocess_safe_env

    @property
    def version(self):
        raise NotImplementedError()

    def copy_for_configured(self, name, description, config_schema, _):
        return ExecutorDefinition(
            name=name or self.name,
            config_schema=config_schema,
            resource_fn=self.resource_fn,
            description=description or self.description,
        )


def executor(name=None, config_schema=None, requires_multiprocess_safe_env=True):
    """Define an executor.

    The decorated function should accept an :py:class:`InitExecutorContext` and return an instance
    of :py:class:`Executor`.

    Args:
        name (Optional[str]): The name of the executor.
        config_schema (Optional[ConfigSchema]): The schema for the config. Configuration data available in
            `init_context.executor_config`.
    """
    if callable(name):
        check.invariant(config_schema is None)
        return _ExecutorDecoratorCallable()(name)

    return _ExecutorDecoratorCallable(
        name=name,
        config_schema=config_schema,
        requires_multiprocess_safe_env=requires_multiprocess_safe_env,
    )


class _ExecutorDecoratorCallable:
    def __init__(self, name=None, config_schema=None, requires_multiprocess_safe_env=True):
        self.name = check.opt_str_param(name, "name")
        self.config_schema = config_schema  # type check in definition
        self.requires_multiprocess_safe_env = requires_multiprocess_safe_env

    def __call__(self, fn):
        check.callable_param(fn, "fn")

        if not self.name:
            self.name = fn.__name__

        executor_def = ExecutorDefinition(
            name=self.name,
            config_schema=self.config_schema,
            resource_fn=fn,
            requires_multiprocess_safe_env=self.requires_multiprocess_safe_env,
        )

        update_wrapper(executor_def, wrapped=fn)

        return executor_def


@executor(
    name="in_process",
    config_schema={
        "retries": get_retries_config(),
        "marker_to_close": Field(str, is_required=False),
    },
    requires_multiprocess_safe_env=False,
)
def in_process_executor(init_context):
    """The default in-process executor.

    In most Dagster environments, this will be the default executor. It is available by default on
    any :py:class:`ModeDefinition` that does not provide custom executors. To select it explicitly,
    include the following top-level fragment in config:

    .. code-block:: yaml

        execution:
          in_process:

    Execution priority can be configured using the ``dagster/priority`` tag via solid metadata,
    where the higher the number the higher the priority. 0 is the default and both positive
    and negative numbers can be used.
    """
    from dagster.core.executor.init import InitExecutorContext
    from dagster.core.executor.in_process import InProcessExecutor

    check.inst_param(init_context, "init_context", InitExecutorContext)

    return InProcessExecutor(
        # shouldn't need to .get() here - issue with defaults in config setup
        retries=Retries.from_config(init_context.executor_config.get("retries", {"enabled": {}})),
        marker_to_close=init_context.executor_config.get("marker_to_close"),
    )


@executor(
    name="multiprocess",
    config_schema={
        "max_concurrent": Field(Int, is_required=False, default_value=0),
        "retries": get_retries_config(),
    },
)
def multiprocess_executor(init_context):
    """The default multiprocess executor.

    This simple multiprocess executor is available by default on any :py:class:`ModeDefinition`
    that does not provide custom executors. To select the multiprocess executor, include a fragment
    such as the following in your config:

    .. code-block:: yaml

        execution:
          multiprocess:
            config:
              max_concurrent: 4

    The ``max_concurrent`` arg is optional and tells the execution engine how many processes may run
    concurrently. By default, or if you set ``max_concurrent`` to be 0, this is the return value of
    :py:func:`python:multiprocessing.cpu_count`.

    Execution priority can be configured using the ``dagster/priority`` tag via solid metadata,
    where the higher the number the higher the priority. 0 is the default and both positive
    and negative numbers can be used.
    """
    from dagster.core.executor.init import InitExecutorContext
    from dagster.core.executor.multiprocess import MultiprocessExecutor

    check.inst_param(init_context, "init_context", InitExecutorContext)

    return MultiprocessExecutor(
        pipeline=init_context.pipeline,
        max_concurrent=init_context.executor_config["max_concurrent"],
        retries=Retries.from_config(init_context.executor_config["retries"]),
    )


default_executors = [in_process_executor, multiprocess_executor]
