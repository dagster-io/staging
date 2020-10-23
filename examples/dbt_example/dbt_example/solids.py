import random

import matplotlib.pyplot
import pandas
import requests
from dagster_dbt import dbt_cli_run, dbt_cli_test
from dagster_dbt.cli.types import DbtCliOutput

from dagster import Array, AssetMaterialization, EventMetadataEntry, Output, solid

CEREAL_DATASET_URL = "https://gist.githubusercontent.com/mgasner/bd2c0f66dff4a9f01855cfa6870b1fce/raw/2de62a57fb08da7c58d6480c987077cf91c783a1/cereal.csv"

PROFILES_DIR = "~/.dbt"
PROJECT_DIR = "./dbt_example_project"


@solid(config_schema={"url": str, "target_path": str})
def download_file(context) -> str:

    url = context.solid_config["url"]
    target_path = context.solid_config["target_path"]

    with open(target_path, "w") as fd:
        fd.write(requests.get(url).text)

    return target_path


@solid(required_resource_keys={"db"})
def load_cereals_from_csv(context, csv_file_path):
    cereals_df = pandas.read_csv(csv_file_path)
    with context.resources.db.connect() as conn:
        conn.execute("drop table if exists cereals cascade")
        cereals_df.to_sql(name="cereals", con=conn)


run_cereals_models = dbt_cli_run.configured(
    name="run_cereals_models",
    config_or_config_fn={"project-dir": PROJECT_DIR, "profiles-dir": PROFILES_DIR},
)

test_cereals_models = dbt_cli_test.configured(
    name="test_cereals_models",
    config_or_config_fn={"project-dir": PROJECT_DIR, "profiles-dir": PROFILES_DIR},
)


@solid
def materialize_dbt_results(_context, run_results: DbtCliOutput) -> bool:
    for node_result in run_results.result.results:
        yield AssetMaterialization(
            asset_key=node_result.node["unique_id"],
            description=node_result.node["raw_sql"],
            metadata_entries=[
                EventMetadataEntry.float(
                    node_result.execution_time, label="execution_time_seconds"
                ),
                EventMetadataEntry.text(node_result.node["checksum"]["checksum"], label="checksum"),
            ],
        )

    yield Output(True)


@solid(required_resource_keys={"db"})
def analyze_cereals(context, _run_results: DbtCliOutput) -> str:
    with context.resources.db.connect() as conn:
        normalized_cereals = pandas.read_sql("select * from normalized_cereals", con=conn)

    colormap = {"H": "orangered", "C": "cornflowerblue"}
    xs = normalized_cereals["normalized_calories"].apply(lambda x: x + random.random() * 3 - 1.5)
    ys = normalized_cereals["normalized_protein"].apply(lambda x: x + random.random() / 10 - 0.05)
    colors = normalized_cereals["type"].apply(lambda x: colormap[x])
    labels = normalized_cereals["name"]

    fig, ax = matplotlib.pyplot.subplots(figsize=(15, 15))
    ax.scatter(x=xs, y=ys, c=colors, alpha=0.5)
    ax.set_xlabel("calories")
    ax.set_label("protein")

    for x, y, label in zip(xs, ys, labels):
        matplotlib.pyplot.annotate(
            label, (x, y), textcoords="offset points", xytext=(3, 10), ha="left", size=7
        )
    plot_path = "cereal_analysis_{run_id}.pdf".format(run_id=context.run_id)
    fig.savefig(plot_path, bbox_inches="tight")

    return plot_path


@solid(config_schema={"channels": Array(str)}, required_resource_keys={"slack"})
def post_plot_to_slack(context, plot_path):
    context.resources.slack.files_upload(
        channels=context.solid_config["channels"], filename=plot_path
    )
