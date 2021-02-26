import string

from dagster import RunRequest, SkipReason, evaluate_sensor, sensor


@sensor(pipeline_name="does_not_matter")
def my_sensor(_context):
    yield RunRequest(run_key="a", run_config={"foo": "bar"})
    yield RunRequest(run_key="b", run_config={"flufff": "barrr"})


@sensor(pipeline_name="does_not_matter")
def skip_sensor(_context):
    yield SkipReason("some reason")


@sensor(pipeline_name="does_not_matter")
def alphabet_sensor(context):
    if context.last_run_key == "z":
        yield SkipReason("no more letters")
        return

    if not context.last_run_key or context.last_run_key not in string.ascii_lowercase:
        yield RunRequest(run_key="a", run_config={"foo": "bar"})
        return

    run_key_index = string.ascii_lowercase.index(context.last_run_key) + 1
    run_key = string.ascii_lowercase[run_key_index]
    yield RunRequest(run_key=run_key, run_config={"foo": "bar"})


def test_basic_sensor():
    requests = evaluate_sensor(my_sensor)
    assert len(requests) == 2
    assert isinstance(requests[0], RunRequest)
    assert requests[0].run_key == "a"
    assert isinstance(requests[1], RunRequest)
    assert requests[1].run_key == "b"


def test_skip_sensor():
    requests = evaluate_sensor(skip_sensor)
    assert len(requests) == 1
    assert isinstance(requests[0], SkipReason)
    assert requests[0].skip_message == "some reason"


def test_conditional_sensor():
    requests = evaluate_sensor(alphabet_sensor)
    assert len(requests) == 1
    assert isinstance(requests[0], RunRequest)
    assert requests[0].run_key == "a"

    requests = evaluate_sensor(alphabet_sensor, last_run_key="p")
    assert len(requests) == 1
    assert isinstance(requests[0], RunRequest)
    assert requests[0].run_key == "q"

    requests = evaluate_sensor(alphabet_sensor, last_run_key="z")
    assert len(requests) == 1
    assert isinstance(requests[0], SkipReason)
    assert requests[0].skip_message == "no more letters"
