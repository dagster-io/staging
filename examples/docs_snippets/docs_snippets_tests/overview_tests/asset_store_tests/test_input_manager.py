from dagster import execute_pipeline
from docs_snippets.overview.asset_stores.input_manager import my_configurable_pipeline, my_pipeline


def test_hardcoded_input_manager():
    execute_pipeline(my_pipeline)


def test_configurable_input_manager():
    execute_pipeline(
        my_configurable_pipeline,
        run_config={
            "resources": {"my_loader": {"config": {"base_dir": "."}}},
            "solids": {"solid1": {"inputs": {"input1": {"key": "some_key"}}}},
        },
    )
