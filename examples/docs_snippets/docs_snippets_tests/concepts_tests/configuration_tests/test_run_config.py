import yaml
from dagster import execute_pipeline
from dagster.utils import file_relative_path


def test_make_variables_resource_any():
    from docs_snippets.concepts.configuration.make_variables_resource_any import (
        my_pipeline,
    )

    assert execute_pipeline(
        my_pipeline, run_config={"resources": {"variable": {"config": "some_value"}}}
    )


def test_make_variables_resource_config_schema():
    from docs_snippets.concepts.configuration.make_variables_resource_config_schema import (
        my_pipeline,
    )

    with open(
        file_relative_path(
            __file__,
            "../../../docs_snippets/concepts/configuration/make_variables_resource_variables.yaml",
        ),
        "r",
    ) as fd:
        run_config = yaml.safe_load(fd.read())
    assert execute_pipeline(my_pipeline, run_config).success
