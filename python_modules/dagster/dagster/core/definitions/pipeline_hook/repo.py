from time import sleep

from dagster import pipeline, repository, solid
from dagster.core.definitions.pipeline_hook.pipeline_hook import (
    MonitorSensorContext,
    pipeline_failure_sensor,
)
from dagster.core.definitions.resource import ResourceDefinition
from dagster_slack import slack_resource


@pipeline_failure_sensor(
    resource_defs={
        "slack": slack_resource.configured({"token": {"env": "SLACK_DAGSTER_ETL_BOT_TOKEN"}}),
        "base_url": ResourceDefinition.hardcoded_resource("http://localhost:3000"),
    }
)
def slack_on_pipeline_failure(context: MonitorSensorContext):
    """
    things a hook context may need:
    - pipeline_run
    - event
    - resources
    """
    run_page_url = f"{context.resources.base_url}/instance/runs/{context.pipeline_run.run_id}"
    channel = "#yuhan-test"
    message = "\n".join(
        [
            f'Pipeline "{context.pipeline_run.pipeline_name}" failed.',
            f"error: {context.event.message}",
            f"mode: {context.pipeline_run.mode}",
            f"run_page_url: {run_page_url}",
        ]
    )
    context.log(f'Sending slack message to {channel}: "{message}"')

    context.resources.slack.chat_postMessage(
        channel=channel,
        blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": message}}],
    )


@solid
def this_solid_will_fail(context, x):
    raise Exception("i failed")


@solid
def this_solid_will_succeed(_):
    sleep(15)
    # pass


@pipeline
def my_pipeline():
    this_solid_will_fail(this_solid_will_succeed())


@repository
def my_repository():
    return [my_pipeline, slack_on_pipeline_failure]
