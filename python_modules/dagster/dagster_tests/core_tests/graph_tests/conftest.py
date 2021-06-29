import pytest
from dagster.core.test_utils import instance_for_test


@pytest.fixture(scope="module")
def instance():
    with instance_for_test() as instance:
        yield instance
