import pytest
from dagster import pipeline, solid
from dagster.core.errors import DagsterInvalidConfigError
from dagster.core.execution.validate_run_config import validate_run_config


def test_validate_run_config():
    @solid
    def basic():
        pass

    @pipeline
    def basic_pipeline():
        basic()

    validate_run_config(basic_pipeline)

    @solid(config_schema={"foo": str})
    def requires_config(_):
        pass

    @pipeline
    def pipeline_requires_config():
        requires_config()

    validate_run_config(
        pipeline_requires_config, {"solids": {"requires_config": {"config": {"foo": "bar"}}}}
    )

    with pytest.raises(DagsterInvalidConfigError):
        validate_run_config(pipeline_requires_config)
