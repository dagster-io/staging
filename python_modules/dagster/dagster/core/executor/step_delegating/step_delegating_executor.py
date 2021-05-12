import time
from typing import Dict, List

from dagster import check
from dagster.core.events import DagsterEvent, EngineEventData
from dagster.core.execution.context.system import PlanOrchestrationContext
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.execution.plan.step import ExecutionStep
from dagster.core.execution.retries import RetryMode
from dagster.utils.backcompat import experimental

from ..base import Executor
from .step_handler import StepHandler


@experimental
class StepDelegatingExecutor(Executor):
    def __init__(
        self,
        step_handler: StepHandler,
        retries: RetryMode = RetryMode.DISABLED,
        sleep_seconds: float = 0.1,
    ):
        self._step_handler = step_handler
        self._retries = retries
        self._sleep_seconds = sleep_seconds

    @property
    def retries(self):
        return self._retries

    def pop_events(self, instance, run_id) -> List[DagsterEvent]:
        events = instance.logs_after(run_id, self._event_cursor)
        self._event_cursor += len(events)
        return [event.dagster_event for event in events if event.is_dagster_event]

    def execute(self, pipeline_context: PlanOrchestrationContext, execution_plan: ExecutionPlan):
        check.inst_param(pipeline_context, "pipeline_context", PlanOrchestrationContext)
        check.inst_param(execution_plan, "execution_plan", ExecutionPlan)

        self._event_cursor = -1  # pylint: disable=attribute-defined-outside-init

        yield DagsterEvent.engine_event(
            pipeline_context,
            f"Starting execution with step handler {self._step_handler.name}",
            EngineEventData(),
        )

        with execution_plan.start(retry_mode=self.retries) as active_execution:
            stopping = False
            running_steps: Dict[str, ExecutionStep] = {}

            while (not active_execution.is_complete and not stopping) or running_steps:
                if active_execution.check_for_interrupts():
                    yield DagsterEvent.engine_event(
                        pipeline_context,
                        "Core executor: received termination signal - " "forwarding to steps",
                        EngineEventData(),
                    )
                    stopping = True
                    active_execution.mark_interrupted()
                    for step_key in running_steps:
                        ExecuteStepArgs(
                            pipeline_origin=self.pipeline_context.reconstructable_pipeline.get_python_origin(),
                            pipeline_run_id=self.pipeline_context.pipeline_run.run_id,
                            step_keys_to_execute=[step_context.step.key],
                            instance_ref=None,
                            retry_mode=RetryMode.DISABLED,
                            known_state=known_state,
                            should_verify_step=True,
                        )
                        self._step_handler.terminate_steps([step_key])

                events = self._pop_events(
                    pipeline_context.plan_data.instance,
                    pipeline_context.plan_data.pipeline_run.run_id,
                )

                for step in running_steps.values():
                    events.extend(
                        self._step_handler.check_step_health(
                            [pipeline_context.for_step(step)],
                            active_execution.get_known_state(),
                        )
                    )

                for dagster_event in events:
                    yield dagster_event
                    active_execution.handle_event(dagster_event)

                    if (
                        dagster_event.is_step_success
                        or dagster_event.is_step_failure
                        or dagster_event.is_step_skipped
                    ):
                        assert isinstance(dagster_event.step_key, str)
                        del running_steps[dagster_event.step_key]
                        active_execution.verify_complete(pipeline_context, dagster_event.step_key)

                # process skips from failures or uncovered inputs
                for event in active_execution.plan_events_iterator(pipeline_context):
                    yield event

                for step in active_execution.get_steps_to_execute():
                    running_steps[step.key] = step
                    self._step_handler.launch_steps(
                        [pipeline_context.for_step(step)],
                        active_execution.get_known_state(),
                    )

                time.sleep(self._sleep_seconds)
