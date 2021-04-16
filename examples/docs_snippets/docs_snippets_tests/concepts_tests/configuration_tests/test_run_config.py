from dagster import execute_pipeline
from docs_snippets.concepts.configuration.multiple_solids import my_pipeline


def test_multiple_solids_config():
    assert execute_pipeline(
        my_pipeline, run_config={"resources": {"my_str": {"config": "some_value"}}}
    )
