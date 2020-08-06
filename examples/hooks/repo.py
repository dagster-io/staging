from dagster_slack import slack_resource

from dagster import ModeDefinition, failure_hook, pipeline, repository, solid, success_hook


@success_hook(required_resource_keys={'slack'})
def slack_on_success(context):
    message = 'Solid {} finished successfully'.format(context.solid.name)
    context.resources.slack.chat.post_message(channel='#foo', text=message)


@failure_hook(required_resource_keys={'slack'})
def slack_on_failure(context):
    message = 'Solid {} failed'.format(context.solid.name)
    context.resources.slack.chat.post_message(channel='#foo', text=message)


@solid
def a(_):
    pass


@solid
def b(_):
    raise Exception()


@slack_on_failure
@pipeline(
    mode_defs=[
        ModeDefinition('dev', resource_defs={'slack': slack_resource}),
        ModeDefinition('prod', resource_defs={'slack': slack_resource}),
    ]
)
def notif_all():
    # the hook "slack_on_failure" is applied on every solid instance within this pipeline
    a()
    b()


@pipeline(
    mode_defs=[
        ModeDefinition('dev', resource_defs={'slack': slack_resource}),
        ModeDefinition('prod', resource_defs={'slack': slack_resource}),
    ]
)
def selective_notif():
    # only solid "a" triggers hooks: a slack message will be sent when it fails or succeeds
    a.with_hooks({slack_on_failure, slack_on_success})()
    # solid "b" won't trigger any hooks
    b()


@repository
def repo():
    return [notif_all, selective_notif]
