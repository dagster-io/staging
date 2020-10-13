import pandas
import requests
import sqlalchemy
from dagster_dbt import dbt_cli_run, dbt_cli_test

from dagster import ModeDefinition, pipeline, resource, solid

PROFILES_DIR = "~/.dbt"
PROJECT_DIR = "./dbt_example_project"

CEREAL_DATASET_URL = "https://gist.githubusercontent.com/mgasner/bd2c0f66dff4a9f01855cfa6870b1fce/raw/2de62a57fb08da7c58d6480c987077cf91c783a1/cereal.csv"


@resource(config_schema={"db_url": str})
def postgres(context):
    return sqlalchemy.create_engine(context.resource_config["db_url"])


@solid(config_schema={"url": str, "target_path": str})
def download_file(context):

    url = context.solid_config["url"]
    target_path = context.solid_config["target_path"]

    with open(target_path, "w") as fd:
        fd.write(requests.get(url).text)

    return target_path


@solid(required_resource_keys={"db"})
def load_cereals_from_csv(context, csv_file_path):
    cereals_df = pandas.read_csv(csv_file_path)
    with context.resources.db.connect() as conn:
        cereals_df.to_sql("cereals", conn, if_exists="replace")


# Solid Configuration
run = dbt_cli_run.configured(
    name="run", config_or_config_fn={"project-dir": PROJECT_DIR, "profiles-dir": PROFILES_DIR},
)

test = dbt_cli_test.configured(
    name="test", config_or_config_fn={"project-dir": PROJECT_DIR, "profiles-dir": PROFILES_DIR},
)


@pipeline(mode_defs=[ModeDefinition(resource_defs={"db": postgres})])
def dbt_example_pipeline():
    loaded = load_cereals_from_csv(download_file())
    run_results = run(start_after=test(start_after=loaded))
