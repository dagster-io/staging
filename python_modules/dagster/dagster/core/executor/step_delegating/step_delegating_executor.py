import time
from enum import Enum
from typing import Dict, List

from dagster import check
from dagster.core.events import DagsterEvent, EngineEventData
from dagster.core.execution.context.system import PlanOrchestrationContext
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.execution.plan.state import KnownExecutionState
from dagster.core.execution.plan.step import ExecutionStep
from dagster.core.execution.retries import RetryMode
from dagster.grpc.types import ExecuteStepArgs
from dagster.utils.backcompat import experimental

from ..base import Executor
from . import StepHandler


class StepIsolationMode(Enum):
    DISABLED = "DISABLED"
    ENABLED = "ENABLED"


@experimental
class StepDelegatingExecutor(Executor):
    def __init__(
        self,
        step_handler: StepHandler,
        step_isolation: StepIsolationMode = StepIsolationMode.ENABLED,
        retries: RetryMode = RetryMode.DISABLED,
        sleep_seconds: float = 0.1,
    ):

        self._step_handler = step_handler
        self._step_isolation = step_isolation
        self._retries = retries
        self._sleep_seconds = sleep_seconds

    @property
    def retries(self):
        return self._retries

    def _pop_events(self, instance, run_id) -> List[DagsterEvent]:
        events = instance.logs_after(run_id, self._event_cursor)
        self._event_cursor += len(events)
        return [event.dagster_event for event in events if event.is_dagster_event]

    def _get_execute_step_args(
        self, pipeline_context, step_keys_to_execute, known_state
    ) -> ExecuteStepArgs:
        return ExecuteStepArgs(
            pipeline_origin=pipeline_context.reconstructable_pipeline.get_python_origin(),
            pipeline_run_id=pipeline_context.pipeline_run.run_id,
            step_keys_to_execute=step_keys_to_execute,
            instance_ref=None,
            retry_mode=self.retries,
            known_state=known_state,
            should_verify_step=True,
        )

    def execute(self, pipeline_context: PlanOrchestrationContext, execution_plan: ExecutionPlan):
        check.inst_param(pipeline_context, "pipeline_context", PlanOrchestrationContext)
        check.inst_param(execution_plan, "execution_plan", ExecutionPlan)

        self._event_cursor = -1  # pylint: disable=attribute-defined-outside-init

        yield DagsterEvent.engine_event(
            pipeline_context,
            f"Starting execution with step handler {self._step_handler.name}",
            EngineEventData(),
        )

        if self._step_isolation == StepIsolationMode.ENABLED:
            yield from self._execute_isolated(pipeline_context, execution_plan)
        elif self._step_isolation == StepIsolationMode.DISABLED:
            yield from self._execute_non_isolated(pipeline_context, execution_plan)
        else:
            raise NotImplementedError(f"Isolation mode {self._step_isolation} not supported")

    def _execute_isolated(
        self, pipeline_context: PlanOrchestrationContext, execution_plan: ExecutionPlan
    ):
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
                        self._step_handler.terminate_step(
                            self._get_execute_step_args(
                                pipeline_context, [step_key], active_execution.get_known_state()
                            )
                        )

                events = self._pop_events(
                    pipeline_context.plan_data.instance,
                    pipeline_context.plan_data.pipeline_run.run_id,
                )

                for step_key in running_steps:
                    events.extend(
                        self._step_handler.check_step_health(
                            self._get_execute_step_args(
                                pipeline_context, [step_key], active_execution.get_known_state()
                            )
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
                    self._step_handler.launch_step(
                        self._get_execute_step_args(
                            pipeline_context, [step.key], active_execution.get_known_state()
                        )
                    )

                time.sleep(self._sleep_seconds)

    def _execute_non_isolated(
        self, pipeline_context: PlanOrchestrationContext, execution_plan: ExecutionPlan
    ):
        yield DagsterEvent.engine_event(
            pipeline_context,
            "Launching all steps",
            EngineEventData(),
        )
        self._step_handler.launch_step(
            self._get_execute_step_args(
                pipeline_context,
                execution_plan.step_keys_to_execute,
                KnownExecutionState.derive_from_logs([]),
            )
        )

        running_steps = set(execution_plan.step_keys_to_execute)

        while running_steps:
            events = self._pop_events(
                pipeline_context.plan_data.instance,
                pipeline_context.plan_data.pipeline_run.run_id,
            )

            for step_key in running_steps:
                events.extend(
                    self._step_handler.check_step_health(
                        self._get_execute_step_args(pipeline_context, [step_key], None)
                    )
                )

            for dagster_event in events:
                yield dagster_event

                if (
                    dagster_event.is_step_success
                    or dagster_event.is_step_failure
                    or dagster_event.is_step_skipped
                ):
                    assert isinstance(dagster_event.step_key, str)
                    running_steps.discard(dagster_event.step_key)

            time.sleep(self._sleep_seconds)
