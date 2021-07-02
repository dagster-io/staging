import subprocess

pytest_plugins = ["dagster_test.fixtures"]


def list_contexts():
    return subprocess.check_output(["docker", "context", "ls"]).decode()


def test_docker_context(docker_context):
    assert docker_context in list_contexts()


def test_docker_context_cm(docker_context_cm):
    with docker_context_cm() as docker_context:
        assert docker_context in list_contexts()


def test_docker_context_cm_name(docker_context_cm):
    assert "foo" not in list_contexts()
    with docker_context_cm(name="foo"):
        assert "foo" in list_contexts()
    assert "foo" not in list_contexts()
