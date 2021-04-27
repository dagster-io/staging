import multiprocessing
import os
import sys
from typing import List

from dagster import EventMetadataEntry, check
from dagster.core.events import DagsterEvent, EngineEventData
from dagster.core.execution.api import create_execution_plan, execute_plan_iterator
from dagster.core.execution.plan.objects import StepFailureData
from dagster.core.executor.child_process_executor import (
    ChildProcessCommand,
    ChildProcessCrashException,
    ChildProcessEvent,
    ChildProcessSystemErrorEvent,
    execute_child_process_command,
)
from dagster.core.instance import DagsterInstance
from dagster.utils import start_termination_thread
from dagster.utils.error import serializable_error_info_from_exc_info

# from .. import CoreExecutor
from . import StepHandler

DELEGATE_MARKER = "multiprocess_subprocess_init"


class MultiprocessExecutorChildProcessCommand(ChildProcessCommand):
    def __init__(
        self,
        run_config,
        pipeline_run,
        step_key,
        instance_ref,
        term_event,
        recon_pipeline,
        retry_mode,
        known_state,
    ):
        self.run_config = run_config
        self.pipeline_run = pipeline_run
        self.step_key = step_key
        self.instance_ref = instance_ref
        self.term_event = term_event
        self.recon_pipeline = recon_pipeline
        self.retry_mode = retry_mode
        self.known_state = known_state

    def execute(self):
        pipeline = self.recon_pipeline
        with DagsterInstance.from_ref(self.instance_ref) as instance:
            start_termination_thread(self.term_event)
            execution_plan = create_execution_plan(
                pipeline=pipeline,
                run_config=self.run_config,
                mode=self.pipeline_run.mode,
                step_keys_to_execute=[self.step_key],
                known_state=self.known_state,
            )

            yield instance.report_engine_event(
                "Executing step {} in subprocess".format(self.step_key),
                self.pipeline_run,
                EngineEventData(
                    [
                        EventMetadataEntry.text(str(os.getpid()), "pid"),
                        EventMetadataEntry.text(self.step_key, "step_key"),
                    ],
                    marker_end=DELEGATE_MARKER,
                ),
                MultiprocessStepHandler,
                self.step_key,
            )

            yield from execute_plan_iterator(
                execution_plan,
                pipeline,
                self.pipeline_run,
                run_config=self.run_config,
                retry_mode=self.retry_mode.for_inner_plan(),
                instance=instance,
            )


class MultiprocessStepHandler(StepHandler):
    def __init__(self):
        super().__init__()
        self._active_iters = {}
        self._errors = {}
        self._term_events = {}
        self._step_handles = {}

    @property
    def name(self):
        return "multiprocess"

    def launch_steps(self, pipeline, step_context, step, known_state):
        self._term_events[step.key] = multiprocessing.Event()
        self._active_iters[step.key] = self._execute_step_out_of_process(
            pipeline,
            step_context,
            step,
            self._errors,
            self._term_events,
            known_state,
        )
        self._step_handles[step.key] = step.handle

    def terminate_steps(self, step_keys: List[str]):
        for step_key in step_keys:
            self._term_events[step_key].set()

    def _execute_step_out_of_process(
        self, pipeline, step_context, step, errors, term_events, known_state
    ):
        command = MultiprocessExecutorChildProcessCommand(
            run_config=step_context.run_config,
            pipeline_run=step_context.pipeline_run,
            step_key=step.key,
            instance_ref=step_context.instance.get_ref(),
            term_event=term_events[step.key],
            recon_pipeline=pipeline,
            retry_mode=self.retries,
            known_state=known_state,
        )

        yield DagsterEvent.engine_event(
            step_context,
            "Launching subprocess for {}".format(step.key),
            EngineEventData(),
            step_handle=step.handle,
        )

        for ret in execute_child_process_command(command):
            if ret is None or isinstance(ret, DagsterEvent):
                yield ret
            elif isinstance(ret, ChildProcessEvent):
                if isinstance(ret, ChildProcessSystemErrorEvent):
                    errors[ret.pid] = ret.error_info
            else:
                check.failed("Unexpected return value from child process {}".format(type(ret)))

    def pop_events(self):
        for key, step_iter in self._active_iters.items():
            try:
                event_or_none = next(step_iter)
                if event_or_none is None:
                    continue
                else:
                    yield event_or_none

            except ChildProcessCrashException as crash:
                serializable_error = serializable_error_info_from_exc_info(sys.exc_info())
                yield DagsterEvent.engine_event(
                    self._pipeline_context,
                    (
                        "Multiprocess executor: child process for step {step_key} "
                        "unexpectedly exited with code {exit_code}"
                    ).format(step_key=key, exit_code=crash.exit_code),
                    EngineEventData.engine_error(serializable_error),
                    step_handle=self._step_handles[key],
                )
                step_failure_event = DagsterEvent.step_failure_event(
                    step_context=self._pipeline_context.for_step(self._step_handles[key]),
                    step_failure_data=StepFailureData(
                        error=serializable_error, user_failure_data=None
                    ),
                )
                yield step_failure_event
            except StopIteration:
                pass

    def check_step_health(self, step):
        pass
