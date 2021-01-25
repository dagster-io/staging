from typing import Callable

from dagster.core.definitions import failure_hook
from dagster.core.definitions.decorators.hook import failure_message
from dagster.core.execution.context.system import HookContext


def trigger_pagerduty_alert_on_failure(
    severity: str, message_fn: Callable[[HookContext], str] = failure_message
):
    @failure_hook(required_resource_keys={'pagerduty'})
    def _hook(context: HookContext):
        context.resources.pagerduty.EventV2_create(
            summary=message_fn(context), source="TODO", severity=severity, dedup_key="TODO",
        )

    return _hook
