from dagster_slack import slack_resource

from dagster import ModeDefinition, pipeline

from .resources import postgres
from .solids import (
    analyze_cereals,
    download_file,
    load_cereals_from_csv,
    post_plot_to_slack,
    run_cereals_models,
    test_cereals_models,
)


@pipeline(mode_defs=[ModeDefinition(resource_defs={"db": postgres, "slack": slack_resource})])
def dbt_example_pipeline():
    loaded = load_cereals_from_csv(download_file())
    run_results = run_cereals_models(start_after=loaded)
    test_cereals_models(start_after=run_results)
    post_plot_to_slack(analyze_cereals(run_results))
