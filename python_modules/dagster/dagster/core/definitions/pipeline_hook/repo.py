# from time import sleep

from dagster import pipeline, repository, solid
from dagster.core.definitions.pipeline_hook.pipeline_hook import PipelineHookContext, pipeline_hook
from dagster.core.definitions.resource import ResourceDefinition
from dagster.core.storage.pipeline_run import PipelineRunStatus, PipelineRunsFilter
from dagster_slack import slack_resource


@solid
def this_solid_will_fail(context, x):
    raise Exception("i failed")


@solid
def this_solid_will_succeed(_):
    # sleep(30)
    pass


@pipeline
def my_pipeline():
    this_solid_will_fail(this_solid_will_succeed())


@pipeline_hook(
    filters=PipelineRunsFilter(
        pipeline_name="my_pipeline",
        statuses=[PipelineRunStatus.FAILURE],
    ),
    resource_defs={
        "slack": slack_resource.configured({"token": {"env": "SLACK_DAGSTER_ETL_BOT_TOKEN"}}),
        "base_url": ResourceDefinition.hardcoded_resource("https://localhost:3000"),
    },
)
def slack_on_failure(context: PipelineHookContext):
    """
    things a hook context may need:
    - pipeline_name
    - run_id
    - dagit_base_url
    - error?
    - event??????????
    """
    run_page_url = f"{context.resources.base_url}/instance/runs/{context.run_id}"

    channel = "#yuhan-test"
    message = "\n".join(
        [
            f'Pipeline "{context.pipeline_name}" failed.',
            f"pipeline_name: {context.pipeline_name}",
            f"run_id: {context.run_id}",
            f"mode: {context.mode}",
            f"run_page_url: {run_page_url}",
        ]
    )
    context.log.info(f'Sending slack message to {channel}: "{message}"')

    context.resources.slack.chat_postMessage(
        channel=channel,
        blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": message}}],
    )


@repository
def my_repository():
    pipelines = [my_pipeline]
    return pipelines + slack_on_failure.get_defs()
