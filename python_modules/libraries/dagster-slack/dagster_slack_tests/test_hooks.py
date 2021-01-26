from dagster import ModeDefinition, execute_pipeline, pipeline, solid
from dagster_slack import slack_resource
from dagster_slack.hooks import send_slack_message_on_failure, send_slack_message_on_success
from mock import patch


class SomeUserException(Exception):
    pass


@patch("slack.web.base_client.BaseClient._perform_urllib_http_request")
def test_failure_hook_on_solid_instance(mock_urllib_http_request):
    @solid
    def pass_solid(_):
        pass

    @solid
    def fail_solid(_):
        raise SomeUserException()

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"slack": slack_resource})])
    def a_pipeline():
        pass_solid.with_hooks(hook_defs={send_slack_message_on_failure("#foo")})()
        pass_solid.alias("solid_with_hook").with_hooks(
            hook_defs={send_slack_message_on_failure("#foo")}
        )()
        fail_solid.alias("fail_solid_without_hook")()
        fail_solid.with_hooks(hook_defs={send_slack_message_on_failure("#foo")})()

    result = execute_pipeline(
        a_pipeline,
        run_config={
            "resources": {"slack": {"config": {"token": "xoxp-1234123412341234-12341234-1234"}}}
        },
        raise_on_error=False,
    )
    assert not result.success
    assert mock_urllib_http_request.call_count == 1


@patch("slack.web.base_client.BaseClient._perform_urllib_http_request")
def test_failure_hook_decorator(mock_urllib_http_request):
    @solid
    def pass_solid(_):
        pass

    @solid
    def fail_solid(_):
        raise SomeUserException()

    @send_slack_message_on_failure("#foo")
    @pipeline(mode_defs=[ModeDefinition(resource_defs={"slack": slack_resource})])
    def a_pipeline():
        pass_solid()
        fail_solid()
        fail_solid.alias("another_fail_solid")()

    result = execute_pipeline(
        a_pipeline,
        run_config={
            "resources": {"slack": {"config": {"token": "xoxp-1234123412341234-12341234-1234"}}}
        },
        raise_on_error=False,
    )
    assert not result.success
    assert mock_urllib_http_request.call_count == 2


@patch("slack.web.base_client.BaseClient._perform_urllib_http_request")
def test_success_hook_on_solid_instance(mock_urllib_http_request):
    @solid
    def pass_solid(_):
        pass

    @solid
    def fail_solid(_):
        raise SomeUserException()

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"slack": slack_resource})])
    def a_pipeline():
        pass_solid.with_hooks(hook_defs={send_slack_message_on_success("#foo")})()
        pass_solid.alias("solid_with_hook").with_hooks(
            hook_defs={send_slack_message_on_success("#foo")}
        )()
        pass_solid.alias("solid_without_hook")()
        fail_solid.with_hooks(hook_defs={send_slack_message_on_success("#foo")})()

    result = execute_pipeline(
        a_pipeline,
        run_config={
            "resources": {"slack": {"config": {"token": "xoxp-1234123412341234-12341234-1234"}}}
        },
        raise_on_error=False,
    )
    assert not result.success
    assert mock_urllib_http_request.call_count == 2


@patch("slack.web.base_client.BaseClient._perform_urllib_http_request")
def test_success_hook_decorator(mock_urllib_http_request):
    @solid
    def pass_solid(_):
        pass

    @solid
    def fail_solid(_):
        raise SomeUserException()

    @send_slack_message_on_success("#foo")
    @pipeline(mode_defs=[ModeDefinition(resource_defs={"slack": slack_resource})])
    def a_pipeline():
        pass_solid()
        pass_solid.alias("another_pass_solid")()
        fail_solid()

    result = execute_pipeline(
        a_pipeline,
        run_config={
            "resources": {"slack": {"config": {"token": "xoxp-1234123412341234-12341234-1234"}}}
        },
        raise_on_error=False,
    )
    assert not result.success
    assert mock_urllib_http_request.call_count == 2
