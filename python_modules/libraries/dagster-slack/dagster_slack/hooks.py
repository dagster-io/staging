from typing import Callable

from dagster.core.definitions import failure_hook, success_hook
from dagster.core.definitions.decorators.hook import failure_message, success_message
from dagster.core.execution.context.system import HookContext


def send_slack_message_on_failure(
    channel: str, message_fn: Callable[[HookContext], str] = failure_message
):
    @failure_hook(required_resource_keys={"slack"})
    def _hook(context: HookContext):
        context.resources.slack.chat_postMessage(channel=channel, text=message_fn(context))

    return _hook


def send_slack_message_on_success(
    channel: str, message_fn: Callable[[HookContext], str] = success_message
):
    """Create a hook on step success events that will message the given Slack channel.

    Args:
        channel (str): The channel to send the message to (e.g. "#my_channel")
        message_fun (Optional(Callable[[HookContext], str])): Function which takes in the HookContext
            outputs the message you want to send.

    Examples:
        .. code-block:: python

            @slack_on_success("#foo")
            @pipeline(...)
            def my_pipeline():
                pass

        .. code-block:: python

            def my_message_fn(context: HookContext) -> str:
                return "TODO: come up with something somewhat interesting"

            @solid
            def a_solid(context):
                pass

            @pipeline(...)
            def my_pipeline():
                a_solid.with_hooks(hook_defs={slack_on_success("#foo", my_message_fn)})

    """

    @success_hook(required_resource_keys={"slack"})
    def _hook(context: HookContext):
        context.resources.slack.chat_postMessage(channel=channel, text=message_fn(context))

    return _hook
