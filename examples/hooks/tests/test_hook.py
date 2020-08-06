import re

import yaml

from dagster import DagsterEventType, execute_pipeline, file_relative_path

from ..repo import notif_all, selective_notif


def test_notif_all_pipeline():
    with open(file_relative_path(__file__, '../dev.yaml'), 'r',) as fd:
        run_config = yaml.safe_load(fd.read())

    result = execute_pipeline(notif_all, mode='dev', run_config=run_config, raise_on_error=False)
    assert not result.success

    for event in result.event_list:
        if event.is_hook_event:
            if event.event_type == DagsterEventType.HOOK_SKIPPED:
                assert event.logging_tags.get('solid') == 'a'
                assert re.match('Skipped the execution of hook "slack_on_failure"', event.message)
            if event.event_type == DagsterEventType.HOOK_COMPLETED:
                assert event.logging_tags.get('solid') == 'b'
                assert re.match('Finished the execution of hook "slack_on_failure"', event.message)


def test_selective_notif_pipeline():
    with open(file_relative_path(__file__, '../dev.yaml'), 'r',) as fd:
        run_config = yaml.safe_load(fd.read())

    result = execute_pipeline(
        selective_notif, mode='dev', run_config=run_config, raise_on_error=False
    )
    assert not result.success

    for event in result.event_list:
        if event.is_hook_event:
            if event.event_type == DagsterEventType.HOOK_SKIPPED:
                assert event.logging_tags.get('solid') == 'a'
                assert re.match('Skipped the execution of hook "slack_on_failure"', event.message)
            if event.event_type == DagsterEventType.HOOK_COMPLETED:
                assert event.logging_tags.get('solid') == 'a'
                assert re.match('Finished the execution of hook "slack_on_success"', event.message)
