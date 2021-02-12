from dagster import ModeDefinition, ResourceDefinition, execute_pipeline, pipeline, resource, solid
from dagster.core.definitions.decorators.pipeline_hook import pipeline_failure_hook
from dagster.seven import mock
from dagster_slack import slack_resource

# called = {}


@pipeline_failure_hook(required_resource_keys={"slack"})
def slack_message_on_pipeline_failure(context, event):
    message = "pipeline {} failed".format(context.pipeline.get_definition().name)
    context.resources.slack.chat_postMessage(channel="#yuhan-test", text=message)
    context.log.info(f"!!!!!!!!!!!!!! {event.event_specific_data}")


slack_resource_mock = mock.MagicMock()


@resource
def bad_resource(init_context):
    raise Exception("bad resource fn")


@solid
def solid_will_pass(_):
    pass


@solid
def solid_will_fail(_):
    raise Exception()


mode_defs = [
    ModeDefinition("fail_at_resource_init", resource_defs={"slack": bad_resource},),
    ModeDefinition(
        "dev",
        resource_defs={
            "slack": ResourceDefinition.hardcoded_resource(
                slack_resource_mock, "do not send messages in dev"
            )
        },
    ),
    ModeDefinition("prod", resource_defs={"slack": slack_resource.configured({"token": "xoxp-"})},),
]


@slack_message_on_pipeline_failure
@pipeline(mode_defs=mode_defs)
def pipeline_has_failed_solid():
    solid_will_pass()
    solid_will_fail()


@slack_message_on_pipeline_failure
@pipeline(mode_defs=mode_defs)
def pipeline_def_has_error():
    solid_will_pass()
    raise Exception("some pipeline error")


if __name__ == "__main__":
    # execute_pipeline(pipeline_has_failed_solid, mode="dev")

    # pipeline hooks won't take care of errors at pipeline def time
    execute_pipeline(pipeline_def_has_error, mode="dev")

    # TODO this isn't working
    # result = execute_pipeline(pipeline_one, mode="fail_at_resource_init")
