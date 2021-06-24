import os
from typing import Callable, Optional

from dagster.core.definitions.pipeline_sensor import (
    PipelineFailureSensorContext,
    pipeline_failure_sensor,
)
from dagster.core.errors import DagsterInvalidDefinitionError
from slack import WebClient


def _default_failure_message(context: PipelineFailureSensorContext) -> str:
    return "\n".join(
        [
            f"Pipeline {context.pipeline_run.pipeline_name} failed!",
            f"Run ID: {context.pipeline_run.run_id}",
            f"Error: {context.failure_event.message}",
        ]
    )


def make_slack_on_pipeline_failure_sensor(
    channel: str,
    slack_token_name: str = "SLACK_TOKEN",
    message_fn: Callable[[PipelineFailureSensorContext], str] = _default_failure_message,
    name: Optional[str] = None,
    dagit_base_url: Optional[str] = None,
):
    """Create a hook on step failure events that will message the given Slack channel.

    Args:
        channel (str): The channel to send the message to (e.g. "#my_channel")
        slack_token_name (str): The name of the slack token in your environment. Defaults
            to "SLACK_TOKEN".
            Tokens are typically either user tokens or bot tokens. More in the Slack API
            documentation here: https://api.slack.com/docs/token-types
        extra_message_fn (Optional(Callable[[HookContext], str])): Function which takes in the
            ``PipelineFailureSensorContext`` outputs the message you want to send. Defaults to
            the slack block that contains error message, pipeline name, and run ID.
        name: (Optional[str]): The name of the sensor. Defaults to "slack_on_pipeline_failure".
        dagit_base_url: (Optional[str]): The base url of your Dagit instance. Specify this to allow
            messages to include deeplinks to the specific pipeline run that triggered the hook.

    Examples:

        .. code-block:: python

            slack_on_pipeline_failure = make_slack_on_pipeline_failure_sensor("#my_channel")

            @repository
            def my_repo():
                return [my_pipeline + slack_on_pipeline_failure]

        .. code-block:: python

            def my_message_fn(context: PipelineFailureSensorContext) -> str:
                return "Pipeline {pipeline_name} failed! Error: {error}".format(
                    pipeline_name=context.pipeline_run.pipeline_name,
                    error=context.failure_event.message,
                )

            slack_on_pipeline_failure = make_slack_on_pipeline_failure_sensor(
                channel="#my_channel",
                slack_token_name="MY_SLACK_TOKEN",
                message_fn=my_message_fn,
                dagit_base_url="http://mycoolsite.com",
            )


    """

    @pipeline_failure_sensor(name=name)
    def slack_on_pipeline_failure(context: PipelineFailureSensorContext):
        slack_token = os.getenv(slack_token_name)
        if not slack_token:
            raise DagsterInvalidDefinitionError(f'Env variable "{slack_token_name}" is unset.')
        slack_client = WebClient(token=slack_token)

        text = message_fn(context)
        if dagit_base_url:
            text += "\n<{base_url}/instance/runs/{run_id}|View in Dagit>".format(
                base_url=dagit_base_url, run_id=context.pipeline_run.run_id
            )

        slack_client.chat_postMessage(channel=channel, text=text)

    return slack_on_pipeline_failure
