import csv
import os
from collections import OrderedDict
from datetime import datetime

from dagster import execute_pipeline, pipeline, solid


def data_loader(context, path):
    csv_path = os.path.join(os.path.dirname(__file__), path)

    with open(csv_path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info("Read from {path}".format(path=path))
    return lines


def data_materializer(context, filename, value):
    # dynamically generated -- actual path unknown beforehand
    dt_string = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
    csv_path = os.path.join(
        os.path.dirname(__file__),
        "uncommitted/cache/{filename}_{dt}.csv".format(filename=filename, dt=dt_string),
    )
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, "w") as fd:
        fieldnames = list(value[0].keys())
        writer = csv.DictWriter(fd, fieldnames)
        writer.writeheader()
        writer.writerows(value)
    context.log.info("Wrote to {path}".format(path=csv_path))
    return csv_path


@solid
def raw_data(context, source_data_path):
    source_data = data_loader(context, source_data_path)
    # say the data is too large to pass between solids,
    # so we want to materialize it to some external storage and pass the path instead
    csv_path = data_materializer(context, "source_data", source_data)

    return csv_path


@solid
def preprocess(context, source_data_path):
    source_data = data_loader(context, source_data_path)

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
    csv_path = data_materializer(context, "processed", processed)
    return csv_path


@solid
def daily_highs(context, data_path):
    data = data_loader(context, data_path)

    highs = []
    for row in data:
        highs.append(OrderedDict([("date", row["date"]), ("high", row["high"]),]))
    csv_path = data_materializer(context, "highs", sorted(highs, key=lambda x: x["date"]))

    return csv_path


@pipeline
def weather_pipeline_pass_path():
    daily_highs(preprocess(raw_data()))


if __name__ == "__main__":
    # there are a few ways to pass externally stored data (intermediates) between solids
    # (2) path is dynamically generated in a solid
    # - output the string value of the path, not the data object
    # - downstream solids load data using that path

    run_config_local_file = {
        "solids": {
            "raw_data": {
                "inputs": {
                    "source_data_path": {"value": "data/weather_data_nyc_centralpark_2016.csv"}
                },
            },
        }
    }

    execute_pipeline(weather_pipeline_pass_path, run_config_local_file)
