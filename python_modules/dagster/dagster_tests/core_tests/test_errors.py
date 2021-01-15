import sys

from dagster import execute_pipeline, pipeline, solid
from dagster.utils.error import serializable_error_info_from_exc_info


def test_syntax_error_serialized_message():
    serialized_error = None

    try:
        eval(  # pylint: disable=eval-used
            """
foo = bar
            """
        )
    except SyntaxError:
        serialized_error = serializable_error_info_from_exc_info(sys.exc_info())

    assert serialized_error

    assert (
        serialized_error.message
        == """  File "<string>", line 2
    foo = bar
        ^
SyntaxError: invalid syntax
"""
    )


def test_solid_raises_error_stdout():
    @solid
    def fails(_):
        raise ValueError("abc")

    @pipeline
    def my_pipeline():
        fails()

    result = execute_pipeline(my_pipeline, raise_on_error=False)
    assert not result.result_for_solid("fails").success
    error_message = result.result_for_solid("fails").failure_data.error.message
    assert (
        error_message.strip()
        == """
Error occurred during the execution of step:
    step key: "fails"
    solid invocation: "fails"
    solid definition: "fails"

ValueError: abc
""".strip()
    )
