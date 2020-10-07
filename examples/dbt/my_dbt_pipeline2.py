from dagster_dbt import dbt_cli_run, dbt_cli_test

from dagster import pipeline

PROFILES_DIR = "~/.dbt"
PROJECT_DIR = "./dbt_example_project"

run = dbt_cli_run.configured(
    name="run", config_or_config_fn={"project-dir": PROJECT_DIR, "profiles-dir": PROFILES_DIR},
)

test = dbt_cli_test.configured(
    name="test", config_or_config_fn={"project-dir": PROJECT_DIR, "profiles-dir": PROFILES_DIR},
)


@pipeline
def my_pipeline():
    run(start_after=test())
