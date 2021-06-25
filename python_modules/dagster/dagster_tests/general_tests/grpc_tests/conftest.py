import pytest
from dagster.core.test_utils import instance_for_test


# Provide an environmental instance for all grpc tests
@pytest.fixture(scope="package", autouse=True)
def instance():
    with instance_for_test() as instance:
        yield instance
