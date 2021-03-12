from collections import namedtuple
from contextlib import contextmanager

from dagster import check
from dagster.core.execution.api import (
    ExecuteRunWithPlanIterable,
    check_execute_pipeline_args,
    create_execution_plan,
    pipeline_execution_iterator,
)
from dagster.core.execution.build_resources import build_resources
from dagster.core.execution.context_creation_pipeline import PipelineExecutionContextManager
from dagster.core.execution.execution_results import PipelineResult
from dagster.core.instance import DagsterInstance


class ExecuteResult(
    namedtuple(
        "_ExecuteResult",
        "pipeline_run instance pipeline_def mode resource_defs run_config execution_plan "
        "event_list",
    )
):
    def __new__(
        cls, pipeline_run, instance, pipeline_def, mode, run_config, execution_plan, event_list
    ):
        return super(ExecuteResult, cls).__new__(
            cls,
            pipeline_run,
            instance,
            pipeline_def,
            mode,
            pipeline_def.get_mode_definition(mode).resource_defs,
            run_config,
            execution_plan,
            event_list,
        )

    @property
    def event_logs(self):
        return self.instance.all_logs(self.pipeline_run.run_id)

    @contextmanager
    def open_result_context(self):
        resource_configs = self.run_config.get("resources") if self.run_config else None
        with build_resources(
            self.resource_defs, self.instance, resource_configs, self.pipeline_run
        ) as resource_context:
            yield PipelineResult(
                self.pipeline_def,
                self.event_list,
                resource_context,
                self.resource_defs,
                self.mode,
                self.run_config,
                self.pipeline_run,
                self.execution_plan,
            )


def execute(workspace, target, instance, mode=None, run_config=None):
    check.inst_param(instance, "instance", DagsterInstance)
    with workspace.reconstructable_from_target(target) as recon_pipeline:
        pipeline, run_config, mode, _, _, _ = check_execute_pipeline_args(
            pipeline=recon_pipeline,
            run_config=run_config,
            mode=mode,
            preset=None,
            tags=None,
        )

        pipeline_run = instance.create_run_for_pipeline(
            pipeline_def=pipeline.get_definition(), run_config=run_config, mode=mode
        )

        execution_plan = create_execution_plan(
            pipeline,
            run_config=pipeline_run.run_config,
            mode=pipeline_run.mode,
            step_keys_to_execute=pipeline_run.step_keys_to_execute,
        )

        _execute_run_iterable = ExecuteRunWithPlanIterable(
            execution_plan=execution_plan,
            iterator=pipeline_execution_iterator,
            execution_context_manager=PipelineExecutionContextManager(
                execution_plan=execution_plan,
                pipeline_run=pipeline_run,
                instance=instance,
                run_config=pipeline_run.run_config,
            ),
        )

        event_list = list(_execute_run_iterable)

        return ExecuteResult(
            pipeline_run,
            instance,
            pipeline.get_definition(),
            mode,
            run_config,
            execution_plan,
            event_list,
        )
