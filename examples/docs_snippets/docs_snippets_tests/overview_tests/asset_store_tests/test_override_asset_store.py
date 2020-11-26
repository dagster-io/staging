from dagster import execute_pipeline
from docs_snippets.overview.asset_stores.override_asset_store import my_pipeline


def test_override_asset_store():
    execute_pipeline(my_pipeline)
