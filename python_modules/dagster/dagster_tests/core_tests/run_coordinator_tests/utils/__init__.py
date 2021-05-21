import pytest

pytest.register_assert_rewrite(
    "dagster_tests.core_tests.run_coordinator_tests.utils.run_coordinator"
)
from .run_coordinator import TestRunCoordinator  # isort:skip
