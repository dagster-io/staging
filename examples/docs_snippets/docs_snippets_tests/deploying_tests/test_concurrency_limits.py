import os

from dagster.core.instance.ref import InstanceRef
from docs_snippets.deploying.concurrency_limits.concurrency_limits import (  # pylint: disable=import-error
    important_pipeline,
    less_important_schedule,
)


def test_inclusion():
    assert important_pipeline
    assert less_important_schedule


def test_instance_yaml(docs_snippets_folder):
    intance_yaml_folder = os.path.join(
        docs_snippets_folder,
        "deploying",
        "concurrency_limits",
    )
    assert InstanceRef.from_dir(intance_yaml_folder).run_coordinator
