import sys

from dagster.core.code_pointer import load_python_file
from dagster.utils import file_relative_path
from dagster.utils.error import serializable_error_info_from_exc_info


def test_syntax_error_serialized_message():
    serialized_error = None

    bad_file = file_relative_path(__file__, "syntax_error_file.py")

    try:
        load_python_file(bad_file, "")
    except SyntaxError:
        serialized_error = serializable_error_info_from_exc_info(sys.exc_info())

    assert serialized_error

    assert (
        serialized_error.message
        == """  File "{bad_file}", line 3
    for i in :
             ^
SyntaxError: invalid syntax
""".format(
            bad_file=bad_file
        )
    )
