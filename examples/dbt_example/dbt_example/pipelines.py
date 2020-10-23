from dagster import ModeDefinition, pipeline

from .resources import postgres
from .solids import (
    analyze_cereals,
    download_file,
    load_cereals_from_csv,
    materialize_dbt_results,
    run_cereals_models,
    test_cereals_models,
)


@pipeline(mode_defs=[ModeDefinition(resource_defs={"db": postgres})])
def dbt_example_pipeline():
    loaded = load_cereals_from_csv(download_file())
    run_results = run_cereals_models(start_after=loaded)
    test_cereals_models(start_after=run_results)
    analyze_cereals(run_results)
    materialize_dbt_results(run_results)
