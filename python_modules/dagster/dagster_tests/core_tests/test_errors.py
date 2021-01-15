import sys

from dagster import IOManager, ModeDefinition, execute_pipeline, io_manager, pipeline, solid
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


def test_solid_raises_error():
    @solid
    def fails(_):
        raise ValueError("abc")

    @pipeline
    def my_pipeline():
        fails()

    result = execute_pipeline(my_pipeline, raise_on_error=False)
    error_message = result.result_for_solid("fails").failure_data.error.message
    assert (
        error_message.strip()
        == """
Error occurred while executing solid "fails":

ValueError: abc
""".strip()
    )


def test_handle_output_raises_error():
    @io_manager
    def my_io_manager(_):
        class MyIOManager(IOManager):
            def handle_output(self, _context, _obj):
                raise ValueError("abc")

            def load_input(self, _context):
                pass

        return MyIOManager()

    @solid
    def return_one(_):
        return 1

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": my_io_manager})])
    def my_pipeline():
        return_one()

    result = execute_pipeline(my_pipeline, raise_on_error=False)
    error_message = result.result_for_solid("return_one").failure_data.error.message
    assert (
        error_message.strip()
        == """
Error occurred while handling output "result" of step "return_one":

ValueError: abc
""".strip()
    )


def test_load_input_raises_error():
    @io_manager
    def my_io_manager(_):
        class MyIOManager(IOManager):
            def handle_output(self, _context, _obj):
                pass

            def load_input(self, _context):
                raise ValueError("abc")

        return MyIOManager()

    @solid
    def return_one(_):
        return 1

    @solid
    def take_data(context, input1):
        context.log(input1)

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"io_manager": my_io_manager})])
    def my_pipeline():
        take_data(return_one())

    result = execute_pipeline(my_pipeline, raise_on_error=False)
    error_message = result.result_for_solid("take_data").failure_data.error.message
    assert (
        error_message.strip()
        == """
Error occurred while loading input "input1" of step "take_data":

ValueError: abc
""".strip()
    )
