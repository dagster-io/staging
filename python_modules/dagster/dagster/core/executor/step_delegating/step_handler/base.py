import abc
from typing import List, NamedTuple, Optional

from dagster import DagsterEvent, DagsterInstance, check
from dagster.core.execution.retries import RetryMode
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.grpc.types import ExecuteStepArgs
from dagster.serdes.serdes import whitelist_for_serdes


@whitelist_for_serdes
class PersistedStepHanderContext(
    NamedTuple(
        "_PersistedStepHanderContext",
        [
            ("execute_step_args", ExecuteStepArgs),
        ],
    )
):
    def __new__(cls, execute_step_args: ExecuteStepArgs):
        return super(PersistedStepHanderContext, cls).__new__(
            cls, check.inst_param(execute_step_args, "execute_step_args", ExecuteStepArgs)
        )


class StepHandlerContext:
    def __init__(
        self,
        instance: DagsterInstance,
        execute_step_args: ExecuteStepArgs,
        pipeline_run: Optional[PipelineRun] = None,
    ) -> None:
        self._instance = instance
        self._execute_step_args = execute_step_args
        self._pipeline_run = pipeline_run

    @property
    def execute_step_args(self) -> ExecuteStepArgs:
        return self._execute_step_args

    @property
    def pipeline_run(self) -> PipelineRun:
        # lazy load
        if not self._pipeline_run:
            run = self._instance.get_run_by_id(self.execute_step_args.pipeline_run_id)
            self._pipeline_run = run

        return self._pipeline_run

    def to_tuple(self) -> PersistedStepHanderContext:
        return PersistedStepHanderContext(execute_step_args=self._execute_step_args)

    @classmethod
    def from_tuple(cls, instance: DagsterInstance, ctx_tuple: PersistedStepHanderContext):
        return cls(instance=instance, execute_step_args=ctx_tuple.execute_step_args)


class StepHandler(abc.ABC):  # pylint: disable=no-init
    @abc.abstractproperty
    def name(self) -> str:
        pass

    @abc.abstractclassmethod
    def launch_step(cls, step_handler_context: StepHandlerContext) -> List[DagsterEvent]:
        pass

    @abc.abstractclassmethod
    def check_step_health(cls, step_handler_context: StepHandlerContext) -> List[DagsterEvent]:
        pass

    @abc.abstractclassmethod
    def terminate_step(cls, step_handler_context: StepHandlerContext) -> List[DagsterEvent]:
        pass
