import pytest

pytest.register_assert_rewrite("dagster_tests.core_tests.storage_tests.utils.run_storage")
from . import run_storage  # isort:skip
