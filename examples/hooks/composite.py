from dagster_slack import slack_resource

from dagster import (
    ModeDefinition,
    ResourceDefinition,
    composite_solid,
    execute_pipeline,
    pipeline,
    repository,
    solid,
    success_hook,
)

mode_defs = [
    ModeDefinition('dev', resource_defs={'slack': ResourceDefinition.mock_resource()},),
    ModeDefinition('prod', resource_defs={'slack': slack_resource}),
]


@success_hook(required_resource_keys={'slack'})
def slack_on_success(context):
    message = 'Solid {} finished successfully'.format(context.solid.name)
    context.resources.slack.chat.post_message(channel='#foo', text=message)


@solid
def one(_):
    return 1


@solid
def adder(_, arg1, arg2):
    return arg1 + arg2


@solid
def print_value(context, num):
    context.log.info('Get number {}'.format(num))


@composite_solid
def a_plus_a():
    a1 = one.alias('a1')()
    a2 = one.alias('a2')()
    return adder(a1, a2)


@slack_on_success
@pipeline(mode_defs=mode_defs)
def notif_all():
    print_value(a_plus_a())


@pipeline(mode_defs=mode_defs)
def selective_notif():
    print_value(a_plus_a.with_hooks({slack_on_success})())


@repository
def repo():
    return [notif_all, selective_notif]


if __name__ == "__main__":
    execute_pipeline(notif_all, mode='dev')
