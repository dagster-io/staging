import os
from contextlib import ExitStack

from dagster import check
from dagster.core.events import DagsterEvent, EngineEventData
from dagster.core.execution.api import ExecuteRunWithPlanIterable
from dagster.core.execution.compute_logs import compute_log_key_for_steps
from dagster.core.execution.context.system import PlanOrchestrationContext
from dagster.core.execution.context_creation_pipeline import PlanExecutionContextManager
from dagster.core.execution.plan.execute_plan import inner_plan_execution_iterator
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.execution.retries import RetryMode
from dagster.core.storage.captured_log_manager import CapturedLogManager
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
        check.inst_param(pipeline_context, "pipeline_context", PlanOrchestrationContext)
        check.inst_param(execution_plan, "execution_plan", ExecutionPlan)

        step_keys_to_execute = execution_plan.step_keys_to_execute

        yield DagsterEvent.engine_event(
            pipeline_context,
            "Executing steps in process (pid: {pid})".format(pid=os.getpid()),
            event_specific_data=EngineEventData.in_process(os.getpid(), step_keys_to_execute),
        )

        with ExitStack() as stack:
            timer_result = stack.enter_context(time_execution_scope())
            log_manager = pipeline_context.instance.compute_log_manager
            if (
                isinstance(log_manager, CapturedLogManager)
                and not log_manager.should_capture_run_by_step()
            ):
                plan_steps = execution_plan.get_steps_to_execute_in_topo_order()
                log_key = compute_log_key_for_steps(plan_steps)
                stack.enter_context(
                    pipeline_context.instance.compute_log_manager.capture_logs(
                        pipeline_context.pipeline_run.run_id, log_key
                    )
                )
                yield DagsterEvent.capture_logs(
                    pipeline_context, log_key, plan_steps, pipeline_context.pipeline_run
                )

            yield from iter(
                ExecuteRunWithPlanIterable(
                    execution_plan=pipeline_context.execution_plan,
                    iterator=inner_plan_execution_iterator,
                    execution_context_manager=PlanExecutionContextManager(
                        pipeline=pipeline_context.pipeline,
                        retry_mode=pipeline_context.retry_mode,
                        execution_plan=pipeline_context.execution_plan,
                        run_config=pipeline_context.run_config,
                        pipeline_run=pipeline_context.pipeline_run,
                        instance=pipeline_context.instance,
                        raise_on_error=pipeline_context.raise_on_error,
                        output_capture=pipeline_context.output_capture,
                    ),
                )
            )

        yield DagsterEvent.engine_event(
            pipeline_context,
            "Finished steps in process (pid: {pid}) in {duration_ms}".format(
                pid=os.getpid(), duration_ms=format_duration(timer_result.millis)
            ),
            event_specific_data=EngineEventData.in_process(os.getpid(), step_keys_to_execute),
        )
