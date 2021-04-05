from collections import namedtuple

from dagster import check
from dagster.core.instance import DagsterInstance


class InitExecutorContext(
    namedtuple(
        "InitExecutorContext",
        "executor_config instance",
    )
):
    """Executor-specific initialization context.

    Attributes:
        executor_config (dict): The parsed config passed to the executor.
        instance (DagsterInstance): The current instance.
    """

    def __new__(
        cls,
        executor_config,
        instance,
    ):
        return super(InitExecutorContext, cls).__new__(
            cls,
            executor_config=check.dict_param(executor_config, executor_config, key_type=str),
            instance=check.inst_param(instance, "instance", DagsterInstance),
        )
