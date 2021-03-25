import os

from dagster import check
from dagster.core.events import DagsterEvent, EngineEventData
from dagster.core.execution.api import execute_plan_iterator
from dagster.core.execution.context.system import SystemPipelineExecutionContext
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.execution.retries import RetryMode
from dagster.utils.timing import format_duration, time_execution_scope

from .base import Executor


class InProcessExecutor(Executor):
    def __init__(self, retries, marker_to_close):
        self._retries = check.inst_param(retries, "retries", RetryMode)
        self.marker_to_close = check.opt_str_param(marker_to_close, "marker_to_close")

    @property
    def retries(self):
        return self._retries

    def execute(self, pipeline_context, execution_plan):
        check.inst_param(pipeline_context, "pipeline_context", SystemPipelineExecutionContext)
        check.inst_param(execution_plan, "execution_plan", ExecutionPlan)

        step_keys_to_execute = execution_plan.step_keys_to_execute

        yield DagsterEvent.engine_event(
            pipeline_context,
            "Executing steps in process (pid: {pid})".format(pid=os.getpid()),
            event_specific_data=EngineEventData.in_process(os.getpid(), step_keys_to_execute),
        )

        with time_execution_scope() as timer_result:
            yield from execute_plan_iterator(
                execution_plan,
                pipeline_context.pipeline_run,
                pipeline_context.instance,
                pipeline_context.retry_mode,
                pipeline_context.run_config,
                pipeline_context.raise_on_error,
                pipeline_context.output_capture,
            )

        yield DagsterEvent.engine_event(
            pipeline_context,
            "Finished steps in process (pid: {pid}) in {duration_ms}".format(
                pid=os.getpid(), duration_ms=format_duration(timer_result.millis)
            ),
            event_specific_data=EngineEventData.in_process(os.getpid(), step_keys_to_execute),
        )
