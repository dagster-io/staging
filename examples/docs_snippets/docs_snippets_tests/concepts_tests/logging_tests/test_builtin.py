import pytest
import yaml
from dagster import execute_pipeline
from dagster.utils import file_relative_path
from docs_snippets.concepts.logging.logging_modes import hello_modes


def test_hello_modes():
    assert execute_pipeline(hello_modes, mode="local").success
