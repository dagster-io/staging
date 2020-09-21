import csv
import os
from collections import OrderedDict
from datetime import datetime

from dagster import (
    AssetMaterialization,
    EventMetadataEntry,
    Field,
    Selector,
    String,
    dagster_type_loader,
    dagster_type_materializer,
    execute_pipeline,
    pipeline,
    solid,
    usable_as_dagster_type,
)
from dagster.core.definitions.input import InputDefinition
from dagster.core.definitions.output import OutputDefinition


@dagster_type_loader(Selector({"csv": Field(String)}))
def data_loader(context, selector):
    csv_path = os.path.join(os.path.dirname(__file__), selector["csv"])

    with open(csv_path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))
    return LocalDataFrame(lines)


@dagster_type_materializer(Selector({"csv": Field(String)}))
def data_materializer(context, config, value):
    # Materialize LocalDataFrame into a csv file
    csv_path = os.path.join(os.path.dirname(__file__), os.path.abspath(config["csv"]))
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, "w") as fd:
        fieldnames = list(value[0].keys())
        writer = csv.DictWriter(fd, fieldnames)
        writer.writeheader()
        writer.writerows(value)

    context.log.debug("Wrote dataframe as .csv to {path}".format(path=csv_path))
    yield AssetMaterialization(
        "data_frame",
        "LocalDataFrame materialized as csv",
        [
            EventMetadataEntry.path(
                path=csv_path,
                label="data_frame_csv_path",
                description="LocalDataFrame written to csv format",
            )
        ],
    )


@usable_as_dagster_type(
    name="LocalDataFrame", loader=data_loader, materializer=data_materializer,
)
class LocalDataFrame(list):
    pass


@solid(
    input_defs=[InputDefinition(name="source_data", dagster_type=LocalDataFrame)],
    output_defs=[OutputDefinition(name="source_data_df", dagster_type=LocalDataFrame)],
)
def raw_data(_, source_data):
    return LocalDataFrame(source_data)


@solid(
    input_defs=[InputDefinition(name="source_data", dagster_type=LocalDataFrame)],
    output_defs=[OutputDefinition(name="processed", dagster_type=LocalDataFrame)],
)
def preprocess(_, source_data):
    processed = []
    for row in source_data:
        processed.append(
            OrderedDict(
                [
                    ("date", datetime.strptime(row["date"], "%d-%m-%Y")),
                    ("high", row["maximum temperature"]),
                    ("low", row["minimum temperature"]),
                ]
            )
        )
    return LocalDataFrame(processed)


@solid(
    input_defs=[InputDefinition(name="data", dagster_type=LocalDataFrame)],
    output_defs=[OutputDefinition(name="highs", dagster_type=LocalDataFrame)],
)
def daily_highs(_, data):
    highs = []
    for row in data:
        highs.append(OrderedDict([("date", row["date"]), ("high", row["high"]),]))

    return LocalDataFrame(sorted(highs, key=lambda x: x["date"]))


@pipeline
def weather_pipeline_path_hardcoded_in_config():
    daily_highs(preprocess(raw_data()))


if __name__ == "__main__":
    # there are a few ways to pass externally stored data (intermediates) between solids
    # (1) path is hardcoded in run config or (even worse) in solid definition

    run_config_local_file = {
        "solids": {
            "raw_data": {
                "inputs": {"source_data": {"csv": "data/weather_data_nyc_centralpark_2016.csv"}},
                "outputs": [{"source_data_df": {"csv": "data/cache/raw_df.csv"}}],
            },
            "preprocess": {
                # "inputs": {"source_data": {"csv": "data/weather_data_nyc_centralpark_2016.csv"}},
                "outputs": [{"processed": {"csv": "data/cache/processed_df.csv"}}],
            },
            "daily_highs": {
                # "inputs": {"data": {"path": "data/cache/weather_df.csv"}},
                "outputs": [{"highs": {"csv": "data/output.csv"}}],
            },
        }
    }

    execute_pipeline(weather_pipeline_path_hardcoded_in_config, run_config_local_file)
