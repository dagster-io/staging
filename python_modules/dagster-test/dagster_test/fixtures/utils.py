# pylint: disable=unused-argument
import os

import pytest
import requests
from urllib3.util.retry import Retry

BUILDKITE = os.environ.get("BUILDKITE") is not None


def determine_scope(fixture_name, config):
    if config.getoption("--keep-containers-module", None):
        return "module"
    return "function"


@pytest.fixture(scope=determine_scope)
def retrying_requests():
    session = requests.Session()
    session.mount(
        "http://", requests.adapters.HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1))
    )
    yield session


@pytest.fixture(scope=determine_scope)
def test_directory(request):
    yield os.path.dirname(request.fspath)


@pytest.fixture(scope=determine_scope)
def test_id(testrun_uid):
    yield testrun_uid
