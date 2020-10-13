from dagster_dbt import dbt_cli_run

from dagster import pipeline

PROFILES_DIR = "~/.dbt"
PROJECT_DIR = "./dbt_example_project"

run = dbt_cli_run.configured(
    name="run", config_or_config_fn={"project-dir": PROJECT_DIR, "profiles-dir": PROFILES_DIR},
)


@pipeline
def my_pipeline():
    run()
