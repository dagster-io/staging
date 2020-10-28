from collections import defaultdict

from graphql.execution.base import ResolveInfo

from dagster import check
from dagster.core.events import DagsterEventType
from dagster.core.execution.plan.objects import deconstruct_step_key
from dagster.core.host_representation import ExternalExecutionPlan, ExternalPipeline
from dagster.core.instance import DagsterInstance
from dagster.core.storage.tags import RESUME_RETRY_TAG

from .external import get_external_execution_plan_or_raise
from .utils import ExecutionParams


def get_retry_steps_from_execution_plan(instance, execution_plan, parent_run_id):
    check.inst_param(instance, "instance", DagsterInstance)
    check.inst_param(execution_plan, "execution_plan", ExternalExecutionPlan)
    check.opt_str_param(parent_run_id, "parent_run_id")

    if not parent_run_id:
        return execution_plan.step_keys_in_plan

    parent_run = instance.get_run_by_id(parent_run_id)
    parent_run_logs = instance.all_logs(parent_run_id)

    all_steps_in_parent_run_logs = set([])
    failed_steps_in_parent_run_logs = defaultdict(set)
    successful_steps_in_parent_run_logs = defaultdict(set)
    interrupted_steps_in_parent_run_logs = defaultdict(set)
    skipped_steps_in_parent_run_logs = defaultdict(set)

    for record in parent_run_logs:
        if record.dagster_event and record.dagster_event.step_key:
            step_key = record.dagster_event.step_key
            all_steps_in_parent_run_logs.add(step_key)
            resolved_step_key, _, unresolved_step_key = deconstruct_step_key(step_key, None)
            if record.dagster_event_type == DagsterEventType.STEP_FAILURE:
                failed_steps_in_parent_run_logs[unresolved_step_key].add(resolved_step_key)
            if record.dagster_event_type == DagsterEventType.STEP_SUCCESS:
                successful_steps_in_parent_run_logs[unresolved_step_key].add(resolved_step_key)
            if record.dagster_event_type == DagsterEventType.STEP_SKIPPED:
                skipped_steps_in_parent_run_logs[unresolved_step_key].add(resolved_step_key)

    for step_key in all_steps_in_parent_run_logs:
        resolved_step_key, mappable_key, unresolved_step_key = deconstruct_step_key(step_key, None)
        if mappable_key:
            if (
                step_key not in failed_steps_in_parent_run_logs.get(unresolved_step_key, [])
                and step_key not in successful_steps_in_parent_run_logs.get(unresolved_step_key, [])
                and step_key not in skipped_steps_in_parent_run_logs.get(unresolved_step_key, [])
            ):
                interrupted_steps_in_parent_run_logs[unresolved_step_key].add(step_key)
        else:
            if (
                step_key not in failed_steps_in_parent_run_logs
                and step_key not in successful_steps_in_parent_run_logs
                and step_key not in skipped_steps_in_parent_run_logs
            ):
                interrupted_steps_in_parent_run_logs[step_key].add(step_key)

    to_retry = set([])
    to_retry_unresolved = set([])
    execution_deps = execution_plan.execution_deps()
    for step in execution_plan.topological_steps():
        if parent_run.step_keys_to_execute and step.key not in parent_run.step_keys_to_execute:
            continue

        resolved_step_key, mappable_key, unresolved_step_key = deconstruct_step_key(step.key, None)
        if step.key in failed_steps_in_parent_run_logs:
            to_retry.update(failed_steps_in_parent_run_logs.get(step.key))
            to_retry_unresolved.add(unresolved_step_key)
            continue
        # Interrupted steps can occur when graceful cleanup from a step failure fails to run,
        # and a step failure event is not generated
        elif step.key in interrupted_steps_in_parent_run_logs:
            to_retry.update(interrupted_steps_in_parent_run_logs.get(step.key))
            to_retry_unresolved.add(unresolved_step_key)
            continue
        # Missing steps did not execute, e.g. when a run was terminated
        elif step.key not in all_steps_in_parent_run_logs:
            # todo: add comment
            if not mappable_key:
                to_retry.add(step.key)
                to_retry_unresolved.add(unresolved_step_key)
                continue

        step_deps = execution_deps[unresolved_step_key]
        if step_deps.intersection(to_retry_unresolved):  # meep this needs to handlaksdfljsdf
            # this step is downstream of a step we are about to retry
            to_retry.add(unresolved_step_key)

    return list(to_retry)


def compute_step_keys_to_execute(graphene_info, external_pipeline, execution_params):
    check.inst_param(graphene_info, "graphene_info", ResolveInfo)
    check.inst_param(external_pipeline, "external_pipeline", ExternalPipeline)
    check.inst_param(execution_params, "execution_params", ExecutionParams)

    instance = graphene_info.context.instance

    if not execution_params.step_keys and is_resume_retry(execution_params):
        # Get step keys from parent_run_id if it's a resume/retry
        external_execution_plan = get_external_execution_plan_or_raise(
            graphene_info=graphene_info,
            external_pipeline=external_pipeline,
            mode=execution_params.mode,
            run_config=execution_params.run_config,
            step_keys_to_execute=None,
        )
        return get_retry_steps_from_execution_plan(
            instance, external_execution_plan, execution_params.execution_metadata.parent_run_id
        )
    else:
        return execution_params.step_keys


def is_resume_retry(execution_params):
    check.inst_param(execution_params, "execution_params", ExecutionParams)
    return execution_params.execution_metadata.tags.get(RESUME_RETRY_TAG) == "true"
