from dagster import check
from dagster.core.events import DagsterEvent, EngineEventData
from dagster.core.execution.context.system import PlanOrchestrationContext
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.execution.retries import RetryMode
from dagster.utils.backcompat import experimental

from ..base import Executor
from .step_handler import StepHandler


@experimental
class CoreExecutor(Executor):
    def __init__(
        self,
        step_handler: StepHandler,
        retries: RetryMode = RetryMode.DISABLED,
    ):
        self._step_handler = step_handler
        self._retries = retries

    @property
    def retries(self):
        return self._retries

    def execute(self, pipeline_context: PlanOrchestrationContext, execution_plan: ExecutionPlan):
        check.inst_param(pipeline_context, "pipeline_context", PlanOrchestrationContext)
        check.inst_param(execution_plan, "execution_plan", ExecutionPlan)

        yield DagsterEvent.engine_event(
            pipeline_context,
            f"Starting execution with step handler {self._step_handler.name}",
            EngineEventData(),
        )

        instance = pipeline_context.instance
        self._step_handler.initialize(instance, pipeline_context, self.retries)

        with execution_plan.start(retry_mode=self.retries) as active_execution:
            stopping = False
            running_steps = set()

            while (not active_execution.is_complete and not stopping) or running_steps:
                if active_execution.check_for_interrupts():
                    yield DagsterEvent.engine_event(
                        pipeline_context,
                        "Core executor: received termination signal - " "forwarding to steps",
                        EngineEventData(),
                    )
                    stopping = True
                    active_execution.mark_interrupted()
                    for step in running_steps:
                        self._step_handler.terminate_steps([step])

                events = self._step_handler.pop_events()
                for dagster_event in events:
                    yield dagster_event
                    active_execution.handle_event(dagster_event)

                    if (
                        dagster_event.is_step_success
                        or dagster_event.is_step_failure
                        or dagster_event.is_step_skipped
                    ):
                        running_steps.remove(dagster_event.step_key)
                        active_execution.verify_complete(pipeline_context, dagster_event.step_key)

                # process skips from failures or uncovered inputs
                for event in active_execution.plan_events_iterator(pipeline_context):
                    yield event

                for step in active_execution.get_steps_to_execute():
                    running_steps.add(step.key)
                    self._step_handler.launch_steps(
                        pipeline_context,
                        pipeline_context.for_step(step),
                        active_execution.get_known_state(),
                    )

                for step_key in running_steps:
                    self._step_handler.check_step_health(
                        pipeline_context,
                        pipeline_context.for_step(execution_plan.get_step_by_key(step_key)),
                        active_execution.get_known_state(),
                    )

            for step_handle in execution_plan.step_dict:
                active_execution.verify_complete(pipeline_context, step_handle.to_key())
