from typing import Callable, Optional

from dagster import check
from dagster.core.definitions.executor import ExecutorDefinition
from dagster.core.definitions.reconstructable import ReconstructablePipeline
from dagster.core.errors import DagsterInvariantViolationError
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.instance import DagsterInstance
from dagster.core.storage.pipeline_run import PipelineRun, PipelineRunStatus

from .api import ExecuteRunWithPlanIterable, pipeline_execution_iterator
from .context_creation_pipeline import PlanOrchestrationContextManager


def execute_run_host_mode(
    pipeline: ReconstructablePipeline,
    pipeline_run: PipelineRun,
    instance: DagsterInstance,
    get_executor_def_fn: Callable[[Optional[str]], ExecutorDefinition] = None,
    raise_on_error: bool = False,
):
    check.inst_param(pipeline, "pipeline", ReconstructablePipeline)
    check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
    check.inst_param(instance, "instance", DagsterInstance)
    check.opt_callable_param(get_executor_def_fn, "get_executor_def_fn")

    if pipeline_run.status == PipelineRunStatus.CANCELED:
        message = "Not starting execution since the run was canceled before execution could start"
        instance.report_engine_event(
            message,
            pipeline_run,
        )
        raise DagsterInvariantViolationError(message)

    check.invariant(
        pipeline_run.status == PipelineRunStatus.NOT_STARTED
        or pipeline_run.status == PipelineRunStatus.STARTING,
        desc="Pipeline run {} ({}) in state {}, expected NOT_STARTED or STARTING".format(
            pipeline_run.pipeline_name, pipeline_run.run_id, pipeline_run.status
        ),
    )

    if pipeline_run.solids_to_execute:
        pipeline = pipeline.subset_for_execution_from_existing_pipeline(
            pipeline_run.solids_to_execute
        )

    execution_plan_snapshot = instance.get_execution_plan_snapshot(
        pipeline_run.execution_plan_snapshot_id
    )
    execution_plan = ExecutionPlan.rebuild_from_snapshot(
        pipeline_run.pipeline_name,
        execution_plan_snapshot,
    )

    _execute_run_iterable = ExecuteRunWithPlanIterable(
        execution_plan=execution_plan,
        iterator=pipeline_execution_iterator,
        execution_context_manager=PlanOrchestrationContextManager(
            pipeline=pipeline,
            execution_plan=execution_plan,
            run_config=pipeline_run.run_config,
            pipeline_run=pipeline_run,
            instance=instance,
            raise_on_error=raise_on_error,
            get_executor_def_fn=get_executor_def_fn,
            host_mode=True,
        ),
    )
    event_list = list(_execute_run_iterable)
    return event_list
