from typing import Callable, Optional

from dagster.core.definitions import failure_hook
from dagster.core.execution.context.system import HookContext


def _default_summary_fn(context: HookContext) -> str:
    return "Solid {solid_name} on pipeline {pipeline_name} failed!".format(
        solid_name=context.solid.name, pipeline_name=context.pipeline_name,
    )


def _default_dedup_key_fn(context: HookContext) -> str:
    return "{pipeline_name}|{solid_name}".format(
        pipeline_name=context.pipeline_name, solid_name=context.solid.name,
    )


def _default_source_fn(context: HookContext):
    return "{pipeline_name}".format(pipeline_name=context.pipeline_name)


def pagerduty_on_failure(
    severity: str,
    summary_fn: Callable[[HookContext], str] = _default_summary_fn,
    dedup_key_fn: Callable[[HookContext], str] = _default_dedup_key_fn,
    source_fn: Callable[[HookContext], str] = _default_source_fn,
    dagit_base_url: Optional[str] = None,
):
    @failure_hook(required_resource_keys={"pagerduty"})
    def _hook(context: HookContext):
        custom_details = {}
        if dagit_base_url:
            custom_details = {
                "dagit url": "{base_url}instance/runs/{run_id}".format(
                    base_url=dagit_base_url, run_id=context.run_id
                )
            }
        context.resources.pagerduty.EventV2_create(
            summary=summary_fn(context),
            source=source_fn(context),
            severity=severity,
            dedup_key=dedup_key_fn(context),
            custom_details=custom_details,
        )

    return _hook
