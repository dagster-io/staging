# pylint: disable=redefined-outer-name
import subprocess
from contextlib import contextmanager

import pytest


@pytest.fixture
def docker_context(docker_context_cm):
    with docker_context_cm() as context:
        yield context


@pytest.fixture
def docker_context_cm(monkeypatch, test_directory, test_id):
    # Docker contexts are stored in $HOME/.docker/contexts.
    # Our Buildkite containers don't typically have $HOME set by default. When it's not set,
    # `docker create context` will save the context in the current directory but `docker compose`
    # will fail because $HOME isn't set when you try to pass a context.
    monkeypatch.setenv("HOME", str(test_directory))

    @contextmanager
    def context(name=test_id):
        try:
            subprocess.check_call(["docker", "context", "create", name])
            yield name
        finally:
            subprocess.check_call(["docker", "context", "rm", name])

    yield context
